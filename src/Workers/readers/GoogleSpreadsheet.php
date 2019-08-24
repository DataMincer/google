<?php

namespace DataMincerGoogle\Workers\readers;

use League\Csv\Exception;
use League\Csv\Reader;
use DataMincerCore\Exception\PluginException;
use DataMincerCore\Plugin\PluginFieldInterface;
use DataMincerCore\Plugin\PluginWorkerBase;

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
    $client = new \Google_Client();
    $client->setApplicationName($this->name);
    $client->setScopes([\Google_Service_Sheets::SPREADSHEETS_READONLY]);
    $client->setAccessType('offline');
    $client->setAuthConfig('a');
    $service = new \Google_Service_Sheets($client);
    $spreadsheetId = '1SBMpbZBKsD0nEPxfWYC5PuwyQTTmw4aIrgbxfuROKq0';

    $range = "List";
    $response = $service->spreadsheets_values->get($spreadsheetId, $range);
    $values = $response->getValues();
    if (empty($values)) {
      $this->error('No data');
    }
    foreach ($values as $row) {
      yield $this->mergeResult($row, $data, $config);
    }
  }

//  protected function readColumns($columns) {
//    return array_map(function($col) {
//      return is_scalar($col) ?
//        [ 'name' => $col ] + $this->defaultColumnDefinition() :
//        $col + $this->defaultColumnDefinition();
//    }, $columns);
//  }
//
//  protected function defaultColumnDefinition() {
//    return  [
//      'autofill' => FALSE,
//      'default' => '',
//    ];
//  }

  static function getSchemaChildren() {
    return parent::getSchemaChildren() + [
      'name' => [ '_type' => 'partial', '_required' => FALSE, '_partial' => 'field' ],
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
      'name' => 'DataMincer',
      'columns' => [],
    ];
  }

}
