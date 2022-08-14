<?php
declare(strict_types=1);

if (!defined('RD_KAFKA_RESP_ERR__AUTHENTICATION')) {
    require __DIR__ . '/../vendor/kwn/php-rdkafka-stubs/stubs/constants.php';
}

if (!class_exists('RdKafka\Conf')) {
    require __DIR__ . '/../vendor/kwn/php-rdkafka-stubs/stubs/RdKafka/Conf.php';
}

if (!class_exists('RdKafka\KafkaConsumer')) {
    require __DIR__ . '/../vendor/kwn/php-rdkafka-stubs/stubs/RdKafka/KafkaConsumer.php';
}

if (!class_exists('RdKafka\Exception')) {
    require __DIR__ . '/../vendor/kwn/php-rdkafka-stubs/stubs/RdKafka/Exception.php';
}

if (!class_exists('RdKafka\TopicPartition')) {
    require __DIR__ . '/../vendor/kwn/php-rdkafka-stubs/stubs/RdKafka/TopicPartition.php';
}

if (!class_exists('RdKafka\Message')) {
    require __DIR__ . '/../vendor/kwn/php-rdkafka-stubs/stubs/RdKafka/Message.php';
}
