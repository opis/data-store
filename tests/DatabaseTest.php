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

namespace Opis\DataStore\Test;

use Opis\Database\Connection;
use Opis\Database\Database as OpisDatabase;
use Opis\Database\Schema\CreateTable;
use Opis\DataStore\Drivers\Database;
use Opis\DataStore\IDataStore;
use PHPUnit\Framework\TestCase;

class DatabaseTest extends TestCase
{
    /** @var IDataStore */
    protected $store;
    /** @var Connection */
    protected $connection;
    /** @var string */
    protected $file;

    public function setUp()
    {
        $file = $this->file = tempnam(sys_get_temp_dir(), 'db-');

        $connection = $this->connection = new Connection('sqlite:' . $file);

        $db = new OpisDatabase($connection);

        $db->schema()
            ->create('config', function (CreateTable $table) {
                $table->string('key')->notNull()->primary();
                $table->binary('data')->size('big')->defaultValue(null);
            });


        $store = $this->store = new Database($connection);

        $store->write('foo', [
            'bar' => 'BAR',
            'baz' => 'BAZ',
        ]);

        $store->write('bar', [
            'foo' => 'FOO',
        ]);

        $store->write('baz', [
            'foo.bar' => 'FOOBAR',
            'foo.baz' => 'FOOBAZ',
        ]);
    }

    public function tearDown()
    {
        $this->store = null;
        $this->connection->disconnect();
        $this->connection = null;
        @unlink($this->file);
    }

    public function testNotFound()
    {
        $this->assertEquals(null, $this->store->read('goo'));
        $this->assertEquals(false, $this->store->read('goo', false));
        $this->assertEquals(false, $this->store->read('bar.FOO', false));
        $this->assertEquals(false, $this->store->read(['bar', 'FOO'], false));
        $this->assertEquals(false, $this->store->read(['bar', 'foo', 'baz'], false));
    }

    public function testRead()
    {
        $this->assertEquals('BAR', $this->store->read('foo.bar'));
        $this->assertEquals('BAR', $this->store->read(['foo', 'bar']));
        $this->assertEquals(['foo' => 'FOO'], $this->store->read('bar'));
        $this->assertEquals('FOOBAR', $this->store->read(['baz', 'foo.bar']));
        $this->assertEquals(false, $this->store->read(['baz', 'foo', 'bar'], false));
    }

    public function testHas()
    {
        $this->assertTrue($this->store->has('foo.bar'));
        $this->assertFalse($this->store->has('foo.qux'));
        $this->assertTrue($this->store->has(['foo', 'bar']));
        $this->assertFalse($this->store->has(['foo', 'qux']));
        $this->assertTrue($this->store->has(['baz', 'foo.bar']));
        $this->assertFalse($this->store->has(['baz', 'foo', 'bar']));
    }

    public function testWrite()
    {
        $this->store->write('foo.qux', 'QUX');
        $this->assertEquals('QUX', $this->store->read('foo.qux'));
        $this->assertEquals('QUX', $this->store->read(['foo', 'qux']));
        $this->assertEquals([
            'bar' => 'BAR',
            'baz' => 'BAZ',
            'qux' => 'QUX'
        ], $this->store->read('foo'));

        $this->store->write(['baz', 'baz.qux'], 'QUX');
        $this->assertEquals('QUX', $this->store->read(['baz', 'baz.qux']));
    }

    public function testDelete()
    {
        $this->assertTrue($this->store->delete('foo.bar'));
        $this->assertFalse($this->store->has('foo.bar'));

        $this->assertFalse($this->store->delete('qux'));
        $this->assertFalse($this->store->delete('baz.foo.bar'));
        $this->assertTrue($this->store->delete(['baz', 'foo.bar']));
        $this->assertFalse($this->store->has(['baz', 'foo.bar']));
    }
}