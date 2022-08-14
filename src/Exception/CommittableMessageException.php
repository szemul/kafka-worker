<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Exception;

use Throwable;

/**
 * Implementers of this exception should be thrown for errors that should cause the kafka message to be committed
 */
interface CommittableMessageException extends Throwable
{
}
