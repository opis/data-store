<?php
/* ===========================================================================
 * Copyright 2018-2019 Zindex Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ============================================================================ */

namespace Opis\DataStore\Drivers;

use Opis\Database\Connection;
use Opis\Database\Database as OpisDatabase;
use Opis\DataStore\{PathTrait, IDataStore};

class Database implements IDataStore
{
    use PathTrait;

    /** @var OpisDatabase */
    protected $db;
    /** @var string */
    protected $table;
    /** @var string[] */
    protected $columns;
    /** @var array */
    protected $cache = [];

    /**
     * @param Connection $connection
     * @param string $table
     * @param string[] $columns
     */
    public function __construct(Connection $connection, string $table = 'config', array $columns = [])
    {
        $this->db = new OpisDatabase($connection);
        $this->table = $table;
        $this->columns = $columns + [
                'key' => 'key',
                'data' => 'data',
            ];
    }

    /**
     * @param string|string[] $path
     * @param mixed|null $default
     * @return mixed
     */
    public function read($path, $default = null)
    {
        $path = $this->normalizePath($path);
        if (empty($path)) {
            return $default;
        }

        $key = reset($path);

        if (!isset($this->cache[$key])) {
            $this->cache[$key] = new Memory($this->import($this->readData($key)));
        }

        return $this->cache[$key]->read($path, $default);
    }

    /**
     * @param string|string[] $path
     * @param mixed $value
     * @return bool
     */
    public function write($path, $value): bool
    {
        $path = $this->normalizePath($path);
        if (empty($path)) {
            return false;
        }

        $key = reset($path);

        if (!isset($this->cache[$key])) {
            $this->cache[$key] = new Memory($this->import($this->readData($key)));
        }

        $store = $this->cache[$key];

        if (!$store->write($path, $value)) {
            return false;
        }

        return $this->writeData($key, $this->export($store->data()));
    }

    /**
     * @param string|string[] $path
     * @return bool
     */
    public function delete($path): bool
    {
        $path = $this->normalizePath($path);
        if (empty($path)) {
            return false;
        }

        $key = reset($path);

        if (!isset($this->cache[$key])) {
            if (count($path) === 1) {
                return $this->deleteKey($key);
            }
            $this->cache[$key] = new Memory($this->import($this->readData($key)));
        }

        if (count($path) === 1) {
            unset($this->cache[$key]);
            return $this->deleteKey($key);
        }

        $store = $this->cache[$key];

        if (!$store->delete($path)) {
            return false;
        }

        return $this->writeData($key, $this->export($store->data()));
    }

    /**
     * @param string|string[] $path
     * @return bool
     */
    public function has($path): bool
    {
        return $this->read($path, $this) !== $this;
    }

    /**
     * @param string $key
     * @return string|null
     */
    protected function readData(string $key): ?string
    {
        $result = $this->db->from($this->table)
            ->where($this->columns['key'])
            ->is($key)
            ->limit(1)
            ->select([$this->columns['data']])
            ->first();
        
        if (!$result) {
            return null;
        }

        return $result->{$this->columns['data']} ?? null;
    }

    /**
     * @param string $key
     * @param string|null $value
     * @return bool
     */
    protected function writeData(string $key, ?string $value): bool
    {
        $exists = $this->db->from($this->table)
            ->where($this->columns['key'])
            ->is($key)
            ->limit(1)
            ->count() > 0;

        $row = [];
        $row[$this->columns['data']] = $value;

        if ($exists) {
            return $this->db->update($this->table)
                ->where($this->columns['key'])
                ->is($key)
                ->set($row) > 0;
        }

        $row[$this->columns['key']] = $key;

        return $this->db->insert($row)->into($this->table);
    }

    /**
     * @param string $key
     * @return bool
     */
    protected function deleteKey(string $key): bool
    {
        return $this->db->from($this->table)
                ->where($this->columns['key'])
                ->is($key)
                ->delete() > 0;
    }

    /**
     * @param string|null $data
     * @return array|mixed
     */
    protected function import(?string $data)
    {
        if ($data === null) {
            return [];
        }
        return json_decode($data, true);
    }

    /**
     * @param $data
     * @return string|null
     */
    protected function export($data): ?string
    {
        return json_encode($data, JSON_UNESCAPED_SLASHES);
    }
}