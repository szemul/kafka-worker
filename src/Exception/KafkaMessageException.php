<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Exception;

use Exception;
use RdKafka\Message;
use Throwable;

class KafkaMessageException extends Exception
{
    public function __construct(private Message $kafkaMessage, Throwable $previous = null)
    {
        $message = 'Error received in kafka message: ' . $this->kafkaMessage->errstr();
        $code    = $this->kafkaMessage->err;

        parent::__construct($message, $code, $previous);
    }

    public function getKafkaMessage(): Message
    {
        return $this->kafkaMessage;
    }
}
