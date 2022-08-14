<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Test\Doubles;

use RdKafka\Conf;
use Throwable;

class ConfDouble extends Conf
{
    public function __construct(private Throwable $groupInstanceIdException)
    {
        parent::__construct();

        $this->set('group.id', 'test');
    }

    /** @throws Throwable */
    public function set($name, $value)
    {
        if ('group.instance.id' == $name) {
            throw $this->groupInstanceIdException;
        }

        parent::set($name, $value);
    }
}
