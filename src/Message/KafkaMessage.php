<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Message;

use RdKafka\Message;
use Szemul\KafkaWorker\Exception\PayloadDecodingFailedException;
use Szemul\Queue\Message\MessageInterface;

class KafkaMessage implements MessageInterface
{
    private ?string $identifier = null;
    /** @var mixed[] */
    private array $payload;

    /** @throws PayloadDecodingFailedException */
    public function __construct(private Message $message)
    {
        try {
            $this->payload = json_decode($this->message->payload, true, flags: JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new PayloadDecodingFailedException($message, $e);
        }
    }

    public function getPayload(): array
    {
        return $this->payload;
    }

    public function getJobName(): ?string
    {
        return $this->message->topic_name;
    }

    public function getQueueIdentifier(): ?string
    {
        return $this->identifier ?? (string)$this->message->offset;
    }

    public function setQueueIdentifier(string $identifier): static
    {
        $this->identifier = $identifier;

        return $this;
    }
}
