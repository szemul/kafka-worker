<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Exception;

use JsonException;
use RdKafka\Message;
use RuntimeException;

class PayloadDecodingFailedException extends RuntimeException
{
    public function __construct(protected Message $kafkaMessage, protected JsonException $exception)
    {
        parent::__construct(
            'Failed to decode message payload as JSON: ' . $this->exception->getMessage(),
            $this->exception->getCode(),
            $this->exception,
        );
    }

    public function getKafkaMessage(): Message
    {
        return $this->kafkaMessage;
    }
}
