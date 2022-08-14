<?php
declare(strict_types=1);

if (!defined('RD_KAFKA_RESP_ERR__AUTHENTICATION')) {
    require __DIR__ . '/../vendor/kwn/php-rdkafka-stubs/stubs/constants.php';
}

if (!class_exists('RdKafka\Conf')) {
    require __DIR__ . '/../vendor/kwn/php-rdkafka-stubs/stubs/RdKafka/Conf.php';
}
