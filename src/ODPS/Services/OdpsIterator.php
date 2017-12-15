<?php
/************************************/
/**
 * 迭代器遍历
 * $it = new OdpsIterator();
 * $it->rewind();
 * while ($it->valid())
 * {
 *     # $key = $it->key(); 无key
 *     $value = $it->current();
 *     $it->next();
 * }
 * */
namespace ODPS\Services;
use ODPS\Core\OdpsException;

class OdpsIterator implements \Iterator {
    private $cached = array();
    private $lastMarker = null;
    private $maxItems = 10;
    private $callbackFuncArr;

    function __construct($callbackParaArr)
    {
        $this->callbackFuncArr = $callbackParaArr;
        $this->callbackFuncArr["funcParams"]["maxitems"] = $this->maxItems;
    }

    /**
     * 检查当前位置是否有效
     * */
    function valid() {
        return $this->valid;
    }

    /**
     * 返回当前元素的键值，因为为objcet所以没有key
     * */
    function key() {
        throw new OdpsException("Do not support key accessing");
    }

    /**
     * 返回当前元素
     * */
    function current() {
        return current($this->cached);
    }

    /**
     * Return the array "pointer" to the first element
     * PHP's reset() returns false if the array has no elements
     * 返回到迭代器的第一个元素
     */
    function rewind() {
        $this->next();
    }

    /**
     * 向前移动到下一个元素
     * */
    function next() {
        $hasCacheItem = (false !== next($this->cached));

        if ($hasCacheItem) {
            $this->valid = true;
            return;
        } else {

            if ($this->lastMarker === null || $this->lastMarker) {
                $callback = $this->callbackFuncArr;
                $callback["funcParams"]["marker"] = $this->lastMarker;
                $rst = call_user_func(array($callback["obj"], $callback["func"]), $callback["funcParams"]);

                if (isset($rst->Marker)) {
                    $this->lastMarker = (string)$rst->Marker;

                    if (isset($rst->{$callback["itemName"]})) {
                        $this->cached = $rst->{$callback["itemName"]};
                    }
                    $hasCacheItem = (false !== reset($this->cached));
                } else {
                    $this->lastMarker = "";
                }
            }
        }
        $this->valid = $hasCacheItem;
    }

}
