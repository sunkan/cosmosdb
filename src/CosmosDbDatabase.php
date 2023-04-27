<?php

namespace Jupitern\CosmosDb;

use GuzzleHttp\Exception\GuzzleException;

class CosmosDbDatabase
{
	public function __construct(
		private CosmosDb $documentDb,
		private string $databaseId,
	) {}

	/**
	 * @throws GuzzleException
	 */
	public function selectCollection(string $collectionName): CosmosDbCollection|null
	{
		$rid_col = false;
		$object = json_decode($this->documentDb->listCollections($this->databaseId));
		$col_list = $object->DocumentCollections;
		for ($i = 0; $i < count($col_list); $i++) {
			if ($col_list[$i]->id === $collectionName) {
				$rid_col = $col_list[$i]->_rid;
			}
		}

		return $rid_col ? new CosmosDbCollection($this->documentDb, $this->databaseId, $rid_col) : null;
	}


	/**
	 * @throws GuzzleException
	 */
	public function createCollection(string $collectionName, string $partitionKey = null): CosmosDbCollection|null
	{
		$col_body = ["id" => $collectionName];
		if ($partitionKey) {
			$col_body["partitionKey"] = [
				"paths" => [$partitionKey],
				"kind" => "Hash"
			];
		}

		$object = json_decode($this->documentDb->createCollection($this->databaseId, json_encode($col_body)));

		return $object->_rid ? new CosmosDbCollection($this->documentDb, $this->databaseId, $object->_rid) : null;
	}
}
