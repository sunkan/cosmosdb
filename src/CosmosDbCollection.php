<?php

namespace Jupitern\CosmosDb;

class CosmosDbCollection
{
	public function __construct(
		private CosmosDb $documentDb,
		private string $databaseId,
		private string $collectionId,
	) {}

	/**
	 * @param array<array-key, scalar> $params
	 * @return array JSON strings
	 */
	public function query(string $query, array $params = [], bool $isCrossPartition = false, string $partitionValue = null)
	{
		$paramsJson = [];
		foreach ($params as $key => $val) {
			$val = is_int($val) || is_float($val) ? $val : '"' . str_replace('"', '\\"', $val) . '"';

			$paramsJson[] = '{"name": "' . str_replace('"', '\\"', $key) . '", "value": ' . $val . '}';
		}

		$query = '{"query": "' . str_replace('"', '\\"', $query) . '", "parameters": [' . implode(',', $paramsJson) . ']}';

		return $this->documentDb->query($this->databaseId, $this->collectionId, $query, $isCrossPartition, $partitionValue);
	}

	public function getPkRanges(): mixed
	{
		return $this->documentDb->getPkRanges($this->databaseId, $this->collectionId);
	}

	public function getPkFullRange(): mixed
	{
		return $this->documentDb->getPkFullRange($this->databaseId, $this->collectionId);
	}

	/**
	 * @param string $json JSON formatted document
	 * @param array $headers Optional headers to send along with the request
	 */
	public function createDocument(string $json, string $partitionKey = null, array $headers = []): string
	{
		return $this->documentDb->createDocument($this->databaseId, $this->collectionId, $json, $partitionKey, $headers);
	}

	/**
	 * @param string $json JSON formatted document
	 * @param array $headers Optional headers to send along with the request
	 */
	public function replaceDocument(string $documentId, string $json, string $partitionKey = null, array $headers = []): string
	{
		return $this->documentDb->replaceDocument($this->databaseId, $this->collectionId, $documentId, $json, $partitionKey, $headers);
	}

	/**
	 * @param string $documentId document ResourceID (_rid)
	 * @param array $headers Optional headers to send along with the request
	 */
	public function deleteDocument(string $documentId, string $partitionKey = null, array $headers = []): string
	{
		return $this->documentDb->deleteDocument($this->databaseId, $this->collectionId, $documentId, $partitionKey, $headers);
	}

	/*
	  public function createUser($json)
	  {
		return $this->document_db->createUser($this->rid_db, $json);
	  }

	  public function listUsers()
	  {
		return $this->document_db->listUsers($this->rid_db, $rid);
	  }

	  public function deletePermission($uid, $pid)
	  {
		return $this->document_db->deletePermission($this->rid_db, $uid, $pid);
	  }

	  public function listPermissions($uid)
	  {
		return $this->document_db->listPermissions($this->rid_db, $uid);
	  }

	  public function getPermission($uid, $pid)
	  {
		return $this->document_db->getPermission($this->rid_db, $uid, $pid);
	  }
	*/

	public function listStoredProcedures(): string
	{
		return $this->documentDb->listStoredProcedures($this->databaseId, $this->collectionId);
	}

	public function executeStoredProcedure($storedProcedureId, $json): string
	{
		return $this->documentDb->executeStoredProcedure($this->databaseId, $this->collectionId, $storedProcedureId, $json);
	}

	public function createStoredProcedure(string $json): string
	{
		return $this->documentDb->createStoredProcedure($this->databaseId, $this->collectionId, $json);
	}

	public function replaceStoredProcedure($storedProcedureId, $json): string
	{
		return $this->documentDb->replaceStoredProcedure($this->databaseId, $this->collectionId, $storedProcedureId, $json);
	}

	public function deleteStoredProcedure($storeProcedureId): string
	{
		return $this->documentDb->deleteStoredProcedure($this->databaseId, $this->collectionId, $storeProcedureId);
	}

	public function listUserDefinedFunctions(): string
	{
		return $this->documentDb->listUserDefinedFunctions($this->databaseId, $this->collectionId);
	}

	public function createUserDefinedFunction(string $json): string
	{
		return $this->documentDb->createUserDefinedFunction($this->databaseId, $this->collectionId, $json);
	}

	public function replaceUserDefinedFunction(string $udf, string $json): string
	{
		return $this->documentDb->replaceUserDefinedFunction($this->databaseId, $this->collectionId, $udf, $json);
	}

	public function deleteUserDefinedFunction(string $udf): string
	{
		return $this->documentDb->deleteUserDefinedFunction($this->databaseId, $this->collectionId, $udf);
	}

	public function listTriggers(): string
	{
		return $this->documentDb->listTriggers($this->databaseId, $this->collectionId);
	}

	public function createTrigger(string $json): string
	{
		return $this->documentDb->createTrigger($this->databaseId, $this->collectionId, $json);
	}

	public function replaceTrigger(string $triggerId, string $json): string
	{
		return $this->documentDb->replaceTrigger($this->databaseId, $this->collectionId, $triggerId, $json);
	}

	public function deleteTrigger(string $triggerId): string
	{
		return $this->documentDb->deleteTrigger($this->databaseId, $this->collectionId, $triggerId);
	}
}
