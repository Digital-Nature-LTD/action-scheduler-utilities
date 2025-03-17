<?php

namespace DigitalNature\ActionSchedulerUtilities\Helpers;

use ActionScheduler;
use ActionScheduler_Action;
use ActionScheduler_InvalidActionException;
use ActionScheduler_NullAction;
use ActionScheduler_NullSchedule;
use ActionScheduler_StoreSchema;
use DateTime;
use Exception;
use InvalidArgumentException;
use wpdb;


// Exit if accessed directly.
if ( ! defined( 'ABSPATH' ) ) exit;

class ScheduledTaskHelper
{
    /** @var int */
    protected static int $max_index_length = 191;

    /**
     * Search with support for multiple hooks
     *
     * @param array $query
     * @return array
     */
    public static function search(array $query): array
    {
        $data = self::query_actions($query);

        return self::hydrate_actions($data);
    }

    /**
     * Returns the number of results
     *
     * @param array $query
     * @return int
     */
    public static function count(array $query): int
    {
        return self::query_actions($query, 'count');
    }

    /**
     * @param int $timestamp
     * @param string $hook
     * @param array $args
     * @param string $group
     * @param bool $unique
     * @param int $priority
     * @return bool
     */
    public static function add_action(
        int $timestamp,
        string $hook,
        array $args = [],
        string $group = '',
        bool $unique = false,
        int $priority = 10
    ): bool
    {
        if (!as_schedule_single_action($timestamp, $hook, $args, $group, $unique, $priority)) {
            return false;
        }

        return true;
    }

    /**
     * @param string $hook
     * @param array $args
     * @param string $group
     * @param bool $unique
     * @param int $priority
     * @return bool
     */
    public static function run_action(
        string $hook,
        array $args = [],
        string $group = '',
        bool $unique = false,
        int $priority = 10
    ): bool
    {
        if (!as_schedule_single_action(time(), $hook, $args, $group, $unique, $priority)) {
            return false;
        }

        return true;
    }

    /**
     * @param ActionScheduler_Action $action
     * @return int|null
     */
    public static function cancel_action(ActionScheduler_Action $action): ?int
    {
        return as_unschedule_action($action->get_hook(), $action->get_args(), $action->get_group());
    }

    /**
     * @param string $hook
     * @param array $args
     * @param string $group
     * @return int|null
     */
    public static function cancel_action_by_params(string $hook, array $args = [], string $group = ''): ?int
    {
        return as_unschedule_action($hook, $args, $group);
    }

    /**
     * @param int $action_id
     * @return void
     */
    public static function cancel_action_by_id(int $action_id)
    {
        ActionScheduler::store()->cancel_action($action_id);
    }

    /**
     * @param string $hook
     * @param array $args
     * @param string $group
     * @return void
     */
    public static function cancel_all(string $hook, array $args = [], string $group = '')
    {
        as_unschedule_all_actions($hook, $args, $group);
    }



    /**
     * This is almost a direct copy of ActionScheduler_DBStore::query_actions,
     * but it allows hooks to be an array, rather than just a string
     */
    private static function query_actions($query = [], $query_type = 'select')
    {
        /** @var wpdb $wpdb */
        global $wpdb;

        $sql = self::get_query_actions_sql($query, $query_type);

        return ('count' === $query_type) ? $wpdb->get_var($sql) : $wpdb->get_col($sql); // phpcs:ignore WordPress.DB.PreparedSQL.NotPrepared, WordPress.DB.DirectDatabaseQuery.NoSql, WordPress.DB.DirectDatabaseQuery.NoCaching
    }

    /**
     * This is almost a direct copy of ActionScheduler_DBStore::get_query_actions_sql,
     * but it allows hooks to be an array, rather than just a string
     */
    private static function get_query_actions_sql(array $query, $select_or_count = 'select'): ?string
    {

        if (!in_array($select_or_count, array('select', 'count'), true)) {
            throw new InvalidArgumentException(__('Invalid value for select or count parameter. Cannot query actions.', 'action-scheduler'));
        }

        $query = wp_parse_args($query, array(
            'hook' => '',
            'args' => null,
            'partial_args_matching' => 'off', // can be 'like' or 'json'
            'date' => null,
            'date_compare' => '<=',
            'modified' => null,
            'modified_compare' => '<=',
            'group' => '',
            'status' => '',
            'claimed' => null,
            'per_page' => 5,
            'offset' => 0,
            'orderby' => 'date',
            'order' => 'ASC',
        ));

        /** @var wpdb $wpdb */
        global $wpdb;

        $db_server_info = is_callable(array($wpdb, 'db_server_info')) ? $wpdb->db_server_info() : $wpdb->db_version();
        if (false !== strpos($db_server_info, 'MariaDB')) {
            $supports_json = version_compare(
                PHP_VERSION_ID >= 80016 ? $wpdb->db_version() : preg_replace('/[^0-9.].*/', '', str_replace('5.5.5-', '', $db_server_info)),
                '10.2',
                '>='
            );
        } else {
            $supports_json = version_compare($wpdb->db_version(), '5.7', '>=');
        }

        $sql = ('count' === $select_or_count) ? 'SELECT count(a.action_id)' : 'SELECT a.action_id';
        $sql .= " FROM {$wpdb->actionscheduler_actions} a";
        $sql_params = array();

        if (!empty($query['group']) || 'group' === $query['orderby']) {
            $sql .= " LEFT JOIN {$wpdb->actionscheduler_groups} g ON g.group_id=a.group_id";
        }

        $sql .= " WHERE 1=1";

        if (!empty($query['group'])) {
            $sql .= " AND g.slug=%s";
            $sql_params[] = $query['group'];
        }

        // This is the original implementation
        /**
         * if ( ! empty( $query['hook'] ) ) {
         * $sql          .= " AND a.hook=%s";
         * $sql_params[] = $query['hook'];
         * }
         */

        if ($query['hook']) {
            $hooks = (array)$query['hook'];
            $placeholders = array_fill(0, count($hooks), '%s');
            $sql .= ' AND a.hook IN (' . join(', ', $placeholders) . ')';
            $sql_params = array_merge($sql_params, array_values($hooks));
        }

        if (!is_null($query['args'])) {
            switch ($query['partial_args_matching']) {
                case 'json':
                    if (!$supports_json) {
                        throw new \RuntimeException(__('JSON partial matching not supported in your environment. Please check your MySQL/MariaDB version.', 'action-scheduler'));
                    }
                    $supported_types = array(
                        'integer' => '%d',
                        'boolean' => '%s',
                        'double' => '%f',
                        'string' => '%s',
                    );
                    foreach ($query['args'] as $key => $value) {
                        $value_type = gettype($value);
                        if ('boolean' === $value_type) {
                            $value = $value ? 'true' : 'false';
                        }
                        $placeholder = isset($supported_types[$value_type]) ? $supported_types[$value_type] : false;
                        if (!$placeholder) {
                            throw new \RuntimeException(sprintf(
                            /* translators: %s: provided value type */
                                __('The value type for the JSON partial matching is not supported. Must be either integer, boolean, double or string. %s type provided.', 'action-scheduler'),
                                $value_type
                            ));
                        }
                        $sql .= ' AND JSON_EXTRACT(a.args, %s)=' . $placeholder;
                        $sql_params[] = '$.' . $key;
                        $sql_params[] = $value;
                    }
                    break;
                case 'like':
                    foreach ($query['args'] as $key => $value) {
                        $sql .= ' AND a.args LIKE %s';
                        $json_partial = $wpdb->esc_like(trim(json_encode(array($key => $value)), '{}'));
                        $sql_params[] = "%{$json_partial}%";
                    }
                    break;
                case 'off':
                    $sql .= " AND a.args=%s";
                    $sql_params[] = self::get_args_for_query($query['args']);
                    break;
                default:
                    throw new \RuntimeException(__('Unknown partial args matching value.', 'action-scheduler'));
            }
        }

        if ($query['status']) {
            $statuses = (array)$query['status'];
            $placeholders = array_fill(0, count($statuses), '%s');
            $sql .= ' AND a.status IN (' . join(', ', $placeholders) . ')';
            $sql_params = array_merge($sql_params, array_values($statuses));
        }

        if ($query['date'] instanceof DateTime) {
            $date = clone $query['date'];
            $date->setTimezone(new \DateTimeZone('UTC'));
            $date_string = $date->format('Y-m-d H:i:s');
            $comparator = self::validate_sql_comparator($query['date_compare']);
            $sql .= " AND a.scheduled_date_gmt $comparator %s";
            $sql_params[] = $date_string;
        }

        if ($query['modified'] instanceof DateTime) {
            $modified = clone $query['modified'];
            $modified->setTimezone(new \DateTimeZone('UTC'));
            $date_string = $modified->format('Y-m-d H:i:s');
            $comparator = self::validate_sql_comparator($query['modified_compare']);
            $sql .= " AND a.last_attempt_gmt $comparator %s";
            $sql_params[] = $date_string;
        }

        if (true === $query['claimed']) {
            $sql .= ' AND a.claim_id != 0';
        } elseif (false === $query['claimed']) {
            $sql .= ' AND a.claim_id = 0';
        } elseif (!is_null($query['claimed'])) {
            $sql .= ' AND a.claim_id = %d';
            $sql_params[] = $query['claimed'];
        }

        if (!empty($query['search'])) {
            $sql .= ' AND (a.hook LIKE %s OR (a.extended_args IS NULL AND a.args LIKE %s) OR a.extended_args LIKE %s';
            for ($i = 0; $i < 3; $i++) {
                $sql_params[] = sprintf('%%%s%%', $query['search']);
            }

            $search_claim_id = (int)$query['search'];
            if ($search_claim_id) {
                $sql .= ' OR a.claim_id = %d';
                $sql_params[] = $search_claim_id;
            }

            $sql .= ')';
        }

        if ('select' === $select_or_count) {
            if ('ASC' === strtoupper($query['order'])) {
                $order = 'ASC';
            } else {
                $order = 'DESC';
            }
            switch ($query['orderby']) {
                case 'hook':
                    $sql .= " ORDER BY a.hook $order";
                    break;
                case 'group':
                    $sql .= " ORDER BY g.slug $order";
                    break;
                case 'modified':
                    $sql .= " ORDER BY a.last_attempt_gmt $order";
                    break;
                case 'none':
                    break;
                case 'action_id':
                    $sql .= " ORDER BY a.action_id $order";
                    break;
                case 'date':
                default:
                    $sql .= " ORDER BY a.scheduled_date_gmt $order";
                    break;
            }

            if ($query['per_page'] > 0) {
                $sql .= ' LIMIT %d, %d';
                $sql_params[] = $query['offset'];
                $sql_params[] = $query['per_page'];
            }
        }

        if (!empty($sql_params)) {
            $sql = $wpdb->prepare($sql, $sql_params); // phpcs:ignore WordPress.DB.PreparedSQL.NotPrepared
        }

        return $sql;
    }

    /**
     * @param $args
     * @return false|string
     */
    private static function get_args_for_query($args)
    {
        $encoded = wp_json_encode($args);
        if (strlen($encoded) <= self::$max_index_length) {
            return $encoded;
        }
        return self::hash_args($encoded);
    }

    /**
     * @param $args
     * @return string
     */
    private static function hash_args($args): string
    {
        return md5($args);
    }

    private static function validate_sql_comparator($comparison_operator)
    {
        if (in_array($comparison_operator, array('!=', '>', '>=', '<', '<=', '='))) {
            return $comparison_operator;
        }
        return '=';
    }

    /**
     * @param array $actionIds
     * @return array
     */
    private static function hydrate_actions(array $actionIds): array
    {
        $actions = [];

        foreach ($actionIds as $action_id ) {
            try {
                $action = self::fetch_action( $action_id );
            } catch (Exception $e) {
                continue;
            }

            if (is_a($action, 'ActionScheduler_NullAction')) {
                continue;
            }

            $actions[$action_id] = $action;
        }

        return $actions;
    }

    /**
     * Retrieve an action.
     *
     * @param int $action_id Action ID.
     *
     * @return ActionScheduler_Action
     */
    private static function fetch_action(int $action_id): ActionScheduler_Action
    {
        /** @var wpdb $wpdb */
        global $wpdb;

        $data = $wpdb->get_row(
            $wpdb->prepare(
                "SELECT a.*, g.slug AS `group` FROM {$wpdb->actionscheduler_actions} a LEFT JOIN {$wpdb->actionscheduler_groups} g ON a.group_id=g.group_id WHERE a.action_id=%d",
                $action_id
            )
        );

        if (empty($data)) {
            return self::get_null_action();
        }

        if (!empty($data->extended_args)) {
            $data->args = $data->extended_args;
            unset( $data->extended_args );
        }

        // Convert NULL dates to zero dates.
        $date_fields = array(
            'scheduled_date_gmt',
            'scheduled_date_local',
            'last_attempt_gmt',
            'last_attempt_gmt',
        );
        foreach ( $date_fields as $date_field ) {
            if ( is_null( $data->$date_field ) ) {
                $data->$date_field = ActionScheduler_StoreSchema::DEFAULT_DATE;
            }
        }

        try {
            $action = self::make_action_from_db_record($data);
        } catch ( ActionScheduler_InvalidActionException $exception ) {
            do_action( 'action_scheduler_failed_fetch_action', $action_id, $exception );
            return self::get_null_action();
        }

        return $action;
    }

    /**
     * Create a null action.
     *
     * @return ActionScheduler_NullAction
     */
    private static function get_null_action(): ActionScheduler_NullAction
    {
        return new ActionScheduler_NullAction();
    }

    /**
     * Create an action from a database record.
     *
     * @param object $data Action database record.
     *
     * @return ActionScheduler_Action
     */
    private static function make_action_from_db_record(object $data): ActionScheduler_Action
    {
        $hook     = $data->hook;
        $args     = json_decode( $data->args, true );
        $schedule = unserialize( $data->schedule ); // phpcs:ignore WordPress.PHP.DiscouragedPHPFunctions.serialize_unserialize

        self::validate_args($args, $data->action_id);
        self::validate_schedule($schedule, $data->action_id);

        if (empty($schedule)) {
            $schedule = new ActionScheduler_NullSchedule();
        }
        $group = $data->group ? $data->group : '';

        return ActionScheduler::factory()->get_stored_action( $data->status, $data->hook, $args, $schedule, $group, $data->priority );
    }

    /**
     * @param $args
     * @param $action_id
     * @return void
     */
    private static function validate_args($args, $action_id ) {
        // Ensure we have an array of args.
        if (!is_array($args)) {
            throw ActionScheduler_InvalidActionException::from_decoding_args( $action_id );
        }

        // Validate JSON decoding if possible.
        if (function_exists( 'json_last_error' ) && JSON_ERROR_NONE !== json_last_error()) {
            throw ActionScheduler_InvalidActionException::from_decoding_args( $action_id, $args );
        }
    }

    /**
     * @param $schedule
     * @param $action_id
     * @return void
     */
    private static function validate_schedule($schedule, $action_id ) {
        if (empty($schedule) || ! is_a($schedule, 'ActionScheduler_Schedule')) {
            throw ActionScheduler_InvalidActionException::from_schedule( $action_id, $schedule );
        }
    }

    /**
     * @param string $hook
     * @param string $linkText
     * @return string
     */
    public static function get_hook_pending_schedule_link_html(string $hook, string $linkText = "View pending scheduled tasks"): string
    {
        return "<a href='/wp-admin/tools.php?page=action-scheduler&status=pending&s={$hook}&action=-1&paged=1&action2=-1'>{$linkText}</a>";
    }
}
