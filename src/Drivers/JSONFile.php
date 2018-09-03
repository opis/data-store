<?php
/* ============================================================================
 * Copyright 2018 Zindex Software
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

class JSONFile extends AbstractFile
{
    const DEFAULT_ENCODE_OPTIONS = JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES;

    /** @var int */
    protected $encodeOptions = 0;

    /** @var bool */
    protected $decodeAsArray = false;

    /**
     * @param string $path
     * @param string $prefix
     * @param bool $decode_assoc
     * @param int $encode_options
     */
    public function __construct(
        string $path,
        string $prefix = '',
        bool $decode_assoc = false,
        int $encode_options = self::DEFAULT_ENCODE_OPTIONS
    ) {
        $this->encodeOptions = $encode_options;
        $this->decodeAsArray = $decode_assoc;
        parent::__construct($path, $prefix, 'json');
    }

    /**
     * @inheritDoc
     */
    protected function import(string $data)
    {
        return json_decode($data, $this->decodeAsArray);
    }

    /**
     * @inheritDoc
     */
    protected function export($data): string
    {
        return json_encode($data, $this->encodeOptions);
    }
}