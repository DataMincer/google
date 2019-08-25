<?php

namespace DataMincerGoogle\Workers\readers;

use Generator;
use Google_Client;
use Google_Exception;
use Google_Service_Sheets;
use DataMincerCore\Exception\PluginException;
use DataMincerCore\Plugin\PluginFieldInterface;
use DataMincerCore\Plugin\PluginWorkerBase;
use Google_Service_Sheets_ValueRange;

/**
 * @property array columns
 * @property PluginFieldInterface name
 * @property PluginFieldInterface credentials
 * @property PluginFieldInterface spreadsheetId
 * @property PluginFieldInterface range
 */
class GoogleSpreadsheet extends PluginWorkerBase {

  protected static $pluginId = 'googlespreadsheet';

  // Used to autofill sparse data
  protected $lastRowBuffer = [];

  /**
   * @inheritDoc
   */
  public function process($config) {
    $data = yield;
    $values = $this->evaluateChildren($data);
    foreach ($this->readTable($values) as $record) {
      $row = [];
      foreach ($this->readColumns($config['columns']) as $field_name => $column_info) {
        if (array_key_exists($column_info['name'], $record)) {
          $value = $record[$column_info['name']];
          if (empty($value)) {
            if ($column_info['autofill']) {
              // Use the last value
              $value = $this->lastRowBuffer[$field_name] ?? $column_info['default'];
            }
            else {
              $value = $column_info['default'];
            }
          }
          $row[$field_name] = $value;
        }
        else {
          $this->error('Column not found in CSV: ' . $column_info['name']);
        }
      }
      $this->lastRowBuffer = $row;
      yield $this->mergeResult($row, $data, $config);
    }
  }

  /**
   * @param $config
   * @return Generator
   * @throws PluginException
   */
  protected function readTable($config) {
    $data = $this->getReader($config)->getValues();

    // Calculate columns count
    $col_count = 0;
    foreach($data as $row) {
      if (count($row) > $col_count) {
        $col_count = count($row);
      }
    }

    if (empty($data)) {
      $this->error('No data');
    }

    $header = [];
    $header_offset = $config['header_offset'];
    $i = 0;
    foreach ($data as $row) {
      $row = $this->padRow($row, $col_count);
      if (!is_null($header_offset) && $i <= $header_offset) {
        if ($header_offset == $i) {
          $header = $row;
        }
      }
      else {
        if (!empty($row)) {
          yield array_combine($header, $row);
        }
        else {
          yield $row;
        }
      }
      $i++;
    }
  }

  /**
   * @param $config
   * @return Google_Service_Sheets_ValueRange
   * @throws PluginException
   */
  protected function getReader($config) {
    $client = new Google_Client();
    $client->setApplicationName($config['name']);
    $client->setScopes([Google_Service_Sheets::SPREADSHEETS_READONLY]);
    $client->setAccessType('offline');
    try {
      $client->setAuthConfig($config['credentials']);
    }
    catch (Google_Exception $e) {
      $this->error($e->getMessage());
    }
    $service = new Google_Service_Sheets($client);
    $spreadsheetId = $config['spreadsheetId'];
    $range = $config['range'];
    $response = $service->spreadsheets_values->get($spreadsheetId, $range);
    return $response;
  }


  protected function padRow($row, $length) {
    if (count($row) < $length) {
      return array_pad($row, $length, NULL);
    }
    return $row;
  }

  protected function readColumns($columns) {
    return array_map(function($col) {
      return is_scalar($col) ?
        [ 'name' => $col ] + $this->defaultColumnDefinition() :
        $col + $this->defaultColumnDefinition();
    }, $columns);
  }

  protected function defaultColumnDefinition() {
    return  [
      'autofill' => FALSE,
      'default' => '',
    ];
  }

  static function getSchemaChildren() {
    return parent::getSchemaChildren() + [
      'name' => [ '_type' => 'partial', '_required' => FALSE, '_partial' => 'field' ],
      'header_offset' => [ '_type' => 'number', '_required' => FALSE ],
      'credentials' => [ '_type' => 'partial', '_required' => TRUE, '_partial' => 'field' ],
      'spreadsheetId' => [ '_type' => 'partial', '_required' => TRUE, '_partial' => 'field' ],
      'range' => [ '_type' => 'partial', '_required' => TRUE, '_partial' => 'field' ],
      'columns' => [ '_type' => 'choice', '_required' => TRUE, '_choices' => [
        'field' => [ '_type' => 'partial', '_partial' => 'field' ],
        'list' => [ '_type' => 'prototype', '_required' => TRUE, '_prototype' => [
          '_type' => 'choice', '_required' => TRUE, '_choices' => [
            'name' => [ '_type' => 'text', '_required' => TRUE ],
            'struct' => ['_type' => 'array', '_required' => TRUE, '_children' => [
              'name' => ['_type' => 'text', '_required' => TRUE],
              'autofill' => [ '_type' => 'boolean', '_required' => FALSE ],
              'default' => [ '_type' => 'text', '_required' => FALSE ],
            ]],
          ],
        ]],
      ]],
    ];
  }

  static function defaultConfig($data = NULL) {
    return parent::defaultConfig($data) + [
      'header_offset' => 0,
      'name' => 'DataMincer',
      'columns' => [],
    ];
  }

}
