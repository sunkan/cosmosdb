<?php

namespace Jupitern\CosmosDb;

use GuzzleHttp\Exception\GuzzleException;
use Psr\Http\Message\ResponseInterface;

class CosmosDb
{
	public array $httpClientOptions = [];

	/**
	 * @param string $host URI of hostname
	 * @param string $privateKey Primary (or Secondary key) private key
	 */
	public function __construct(
		private string $host,
		private string $privateKey
	) {}

	/**
	 * set guzzle http client options using an associative array.
	 *
	 * @param array $options
	 */
	public function setHttpClientOptions(array $options = []): void
	{
		$this->httpClientOptions = $options;
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn783368.aspx
	 * @param string $verb Request Method (GET, POST, PUT, DELETE)
	 * @param string $resourceType Resource Type
	 * @param string $resourceId Resource ID
	 * @return array of Request Headers
	 */
	private function getAuthHeaders(string $verb, string $resourceType, string $resourceId): array
	{
		$x_ms_date = gmdate('D, d M Y H:i:s T', strtotime('+2 minutes'));
		$master = 'master';
		$token = '1.0';
		$x_ms_version = '2018-12-31';

		$key = base64_decode($this->privateKey);
		$stringToSign = $verb . "\n" .
			$resourceType . "\n" .
			$resourceId . "\n" .
			$x_ms_date . "\n" .
			"\n";

		$sig = base64_encode(hash_hmac('sha256', strtolower($stringToSign), $key, true));

		return [
			'Accept' => 'application/json',
			'User-Agent' => 'documentdb.php.sdk/1.0.0',
			'Cache-Control' => 'no-cache',
			'x-ms-date' => $x_ms_date,
			'x-ms-version' => $x_ms_version,
			'authorization' => urlencode("type={$master}&ver={$token}&sig={$sig}")
		];
	}

	/**
	 * @param string $body request body (JSON or QUERY)
	 * @throws GuzzleException
	 */
	private function request(string $path, string $method, array $headers, $body = NULL): ResponseInterface
	{
		$client = new \GuzzleHttp\Client();

		$options = [
			'headers' => $headers,
			'body' => $body,
		];

		return $client->request(
			$method, $this->host . $path, array_merge(
				$options,
				$this->httpClientOptions
			)
		);
	}

	/**
	 * @throws GuzzleException
	 */
	public function selectDB(string $dbName): ?CosmosDbDatabase
	{
		$databaseId = false;
		$object = json_decode($this->listDatabases());

		$dbList = $object->Databases;
		for ($i = 0; $i < count($dbList); $i++) {
			if ($dbList[$i]->id === $dbName) {
				$databaseId = $dbList[$i]->_rid;
			}
		}
		if (!$databaseId) {
			$object = json_decode($this->createDatabase('{"id":"' . $dbName . '"}'));
			$databaseId = $object->_rid;
		}

		return $databaseId ? new CosmosDbDatabase($this, $databaseId) : null;
	}

	/**
	 * @throws GuzzleException
	 */
	public function getInfo(): string
	{
		$headers = $this->getAuthHeaders('GET', '', '');
		$headers['Content-Length'] = '0';
		return $this->request("", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn783363.aspx
	 * @param string $resourceId Resource ID
	 * @param string $collectionId Resource Collection ID
	 * @param string $query Query
	 * @param boolean $isCrossPartition used for cross partition query
	 * @return array JSON response
	 * @throws GuzzleException
	 */
	public function query(string $resourceId, string $collectionId, string $query, bool $isCrossPartition = false, $partitionValue = null): array
	{
		$headers = $this->getAuthHeaders('POST', 'docs', $collectionId);
		$headers['Content-Length'] = strlen($query);
		$headers['Content-Type'] = 'application/query+json';
		$headers['x-ms-max-item-count'] = -1;
		$headers['x-ms-documentdb-isquery'] = 'True';

		if ($isCrossPartition) {
			$headers['x-ms-documentdb-query-enablecrosspartition'] = 'True';
		}

		if ($partitionValue) {
			$headers['x-ms-documentdb-partitionkey'] = '["' . $partitionValue . '"]';
		}
		/*
		 * Fix for https://github.com/jupitern/cosmosdb/issues/21 (credits to https://github.com/ElvenSpellmaker).
		 *
		 * CosmosDB has a max packet size of 4MB and will automatically paginate after that, regardless of x-ms-max-items.
		 * If this is the case, a 'x-ms-continuation'-header will be present in the response headers. The value of this
		 * header will be a continuation token. If this header is detected, we can rerun our query with an additional
		 * 'x-ms-continuation' request header, with the continuation token we received earlier as its value.
		 *
		 * This fix checks if this header is present on the response headers and handles the additional requests, untill
		 * all results are loaded.
		 */
		$results = [];
		try {
			$result = $this->request("/dbs/{$resourceId}/colls/{$collectionId}/docs", "POST", $headers, $query);
			$results[] = $result->getBody()->getContents();
			while ($result->getHeader('x-ms-continuation') !== []) {
				$headers['x-ms-continuation'] = $result->getHeader('x-ms-continuation');
				$result = $this->request("/dbs/{$resourceId}/colls/{$collectionId}/docs", "POST", $headers, $query);
				$results[] = $result->getBody()->getContents();
			}
		}
		catch (\GuzzleHttp\Exception\ClientException $e) {
			$responseError = \json_decode($e->getResponse()->getBody()->getContents());

			// -- Retry the request with PK Ranges --
			// The provided cross partition query can not be directly served by the gateway.
			// This is a first chance (internal) exception that all newer clients will know how to
			// handle gracefully. This exception is traced, but unless you see it bubble up as an
			// exception (which only happens on older SDK clients), then you can safely ignore this message.
			if ($isCrossPartition && $responseError->code === "BadRequest" && strpos($responseError->message, "cross partition query can not be directly served by the gateway") !== false) {
				$headers["x-ms-documentdb-partitionkeyrangeid"] = $this->getPkFullRange($resourceId, $collectionId);
				$result = $this->request("/dbs/{$resourceId}/colls/{$collectionId}/docs", "POST", $headers, $query);
				$results[] = $result->getBody()->getContents();
				while ($result->getHeader('x-ms-continuation') !== []) {
					$headers['x-ms-continuation'] = $result->getHeader('x-ms-continuation');
					$result = $this->request("/dbs/{$resourceId}/colls/{$collectionId}/docs", "POST", $headers, $query);
					$results[] = $result->getBody()->getContents();
				}
			}
			else {
				throw $e;
			}
		}

		return $results;
	}

	/**
	 * @throws GuzzleException
	 */
	public function getPkRanges(string $databaseId, string $collectionId): mixed
	{
		$headers = $this->getAuthHeaders('GET', 'pkranges', $collectionId);
		$headers['Accept'] = 'application/json';
		$headers['x-ms-max-item-count'] = -1;
		$result = $this->request("/dbs/{$databaseId}/colls/{$collectionId}/pkranges", "GET", $headers);
		return json_decode($result->getBody()->getContents());
	}

	/**
	 * @throws GuzzleException
	 */
	public function getPkFullRange($databaseId, $collectionId): string
	{
		$result = $this->getPkRanges($databaseId, $collectionId);
		$ids = \array_column($result->PartitionKeyRanges, "id");
		return $result->_rid . "," . \implode(",", $ids);
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803945.aspx
	 * @throws GuzzleException
	 */
	public function listDatabases(): string
	{
		$headers = $this->getAuthHeaders('GET', 'dbs', '');
		$headers['Content-Length'] = '0';
		return $this->request("/dbs", "GET", $headers)->getBody()->getContents();
	}

	/**
	 *
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803937.aspx
	 * @throws GuzzleException
	 */
	public function getDatabase(string $databaseId): string
	{
		$headers = $this->getAuthHeaders('GET', 'dbs', $databaseId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803954.aspx
	 * @throws GuzzleException
	 */
	public function createDatabase(string $json): string
	{
		$headers = $this->getAuthHeaders('POST', 'dbs', '');
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs", "POST", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803943.aspx
	 * @throws GuzzleException
	 */
	public function replaceDatabase(string $databaseId, string $json): string
	{
		$headers = $this->getAuthHeaders('PUT', 'dbs', $databaseId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}", "PUT", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803942.aspx
	 */
	public function deleteDatabase(string $databaseId): string
	{
		$headers = $this->getAuthHeaders('DELETE', 'dbs', $databaseId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}", "DELETE", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803958.aspx
	 * @throws GuzzleException
	 */
	public function listUsers(string $databaseId): string
	{
		$headers = $this->getAuthHeaders('GET', 'users', $databaseId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/users", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803949.aspx
	 * @throws GuzzleException
	 */
	public function getUser(string $databaseId, string $userId): string
	{
		$headers = $this->getAuthHeaders('GET', 'users', $userId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/users/{$userId}", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803946.aspx
	 * @param string $json JSON request
	 * @throws GuzzleException
	 */
	public function createUser(string $databaseId, string $json): string
	{
		$headers = $this->getAuthHeaders('POST', 'users', $databaseId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/users", "POST", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803941.aspx
	 * @param string $json JSON request
	 * @throws GuzzleException
	 */
	public function replaceUser(string $databaseId, string $userId, string $json): string
	{
		$headers = $this->getAuthHeaders('PUT', 'users', $userId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/users/{$userId}", "PUT", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803953.aspx
	 * @throws GuzzleException
	 */
	public function deleteUser(string $databaseId, string $userId): string
	{
		$headers = $this->getAuthHeaders('DELETE', 'users', $userId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/users/{$userId}", "DELETE", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803935.aspx
	 * @throws GuzzleException
	 */
	public function listCollections(string $databaseId): string
	{
		$headers = $this->getAuthHeaders('GET', 'colls', $databaseId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803951.aspx
	 * @throws GuzzleException
	 */
	public function getCollection(string $databaseId, string $collectionId): string
	{
		$headers = $this->getAuthHeaders('GET', 'colls', $collectionId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803934.aspx
	 * @param string $json JSON request
	 * @throws GuzzleException
	 */
	public function createCollection(string $databaseId, string $json): string
	{
		$headers = $this->getAuthHeaders('POST', 'colls', $databaseId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/colls", "POST", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803953.aspx
	 * @throws GuzzleException
	 */
	public function deleteCollection(string $databaseId, string $collectionId): string
	{
		$headers = $this->getAuthHeaders('DELETE', 'colls', $collectionId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}", "DELETE", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803955.aspx
	 * @throws GuzzleException
	 */
	public function listDocuments(string $databaseId, string $collectionId): string
	{
		$headers = $this->getAuthHeaders('GET', 'docs', $collectionId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/docs", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803957.aspx
	 * @throws GuzzleException
	 */
	public function getDocument(string $databaseId, string $collectionId, string $documentId): string
	{
		$headers = $this->getAuthHeaders('GET', 'docs', $documentId);
		$headers['Content-Length'] = '0';
		$options = array(
			CURLOPT_HTTPHEADER => $headers,
			CURLOPT_HTTPGET => true,
		);
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/docs/{$documentId}", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803948.aspx
	 * @param string $json JSON request
	 * @param array $headers Optional headers to send along with the request
	 * @throws GuzzleException
	 */
	public function createDocument(string $databaseId, string $collectionId, string $json, string $partitionKey = null, array $headers = []): string
	{
		$authHeaders = $this->getAuthHeaders('POST', 'docs', $collectionId);
		$headers = \array_merge($headers, $authHeaders);
		$headers['Content-Length'] = strlen($json);
		if ($partitionKey !== null) {
			$headers['x-ms-documentdb-partitionkey'] = '["' . $partitionKey . '"]';
		}

		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/docs", "POST", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803947.aspx
	 * @param string $json JSON request
	 * @param array $headers Optional headers to send along with the request
	 * @throws GuzzleException
	 */
	public function replaceDocument(string $databaseId, string $collectionId, string $documentId, string $json, string $partitionKey = null, array $headers = []): string
	{
		$authHeaders = $this->getAuthHeaders('PUT', 'docs', $documentId);
		$headers = \array_merge($headers, $authHeaders);
		$headers['Content-Length'] = strlen($json);
		if ($partitionKey !== null) {
			$headers['x-ms-documentdb-partitionkey'] = '["' . $partitionKey . '"]';
		}

		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/docs/{$documentId}", "PUT", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803952.aspx
	 * @param array $headers Optional headers to send along with the request
	 * @throws GuzzleException
	 */
	public function deleteDocument(string $databaseId, string $collectionId, string $documentId, string $partitionKey = null, array $headers = []): string
	{
		$authHeaders = $this->getAuthHeaders('DELETE', 'docs', $documentId);
		$headers = \array_merge($headers, $authHeaders);
		$headers['Content-Length'] = '0';
		if ($partitionKey !== null) {
			$headers['x-ms-documentdb-partitionkey'] = '["' . $partitionKey . '"]';
		}

		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/docs/{$documentId}", "DELETE", $headers)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function listAttachments(string $databaseId, string $collectionId, string $documentId): string
	{
		$headers = $this->getAuthHeaders('GET', 'attachments', $documentId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/docs/{$documentId}/attachments", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function getAttachment(string $databaseId, string $collectionId, string $documentId, string $attachmentId): string
	{
		$headers = $this->getAuthHeaders('GET', 'attachments', $attachmentId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/docs/{$documentId}/attachments/{$attachmentId}", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803933.aspx
	 * @param string $file URL encoded Attachement file (Raw Media)
	 * @throws GuzzleException
	 */
	public function createAttachment(string $databaseId, string $collectionId, string $documentId, string $contentType, string $filename, string $file): string
	{
		$headers = $this->getAuthHeaders('POST', 'attachments', $documentId);
		$headers['Content-Length'] = strlen($file);
		$headers['Content-Type'] = $contentType;
		$headers['Slug'] = urlencode($filename);
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/docs/{$documentId}/attachments", "POST", $headers, $file)->getBody()->getContents();
	}

	/**
	 * @param string $file URL encoded Attachement file (Raw Media)
	 * @throws GuzzleException
	 */
	public function replaceAttachment(
		string $databaseId,
		string $collectionId,
		string $documentId,
		string $attachmentId,
		string $contentType,
		string $filename,
		string $file
	): string {
		$headers = $this->getAuthHeaders('PUT', 'attachments', $attachmentId);
		$headers['Content-Length'] = strlen($file);
		$headers['Content-Type'] = $contentType;
		$headers['Slug'] = urlencode($filename);
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/docs/{$documentId}/attachments/{$attachmentId}", "PUT", $headers, $file)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function deleteAttachment(
		string $databaseId,
		string $collectionId,
		string $documentId,
		string $attachmentId
	): string {
		$headers = $this->getAuthHeaders('DELETE', 'attachments', $attachmentId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/docs/{$documentId}/attachments/{$attachmentId}", "DELETE", $headers)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function listOffers(): string
	{
		$headers = $this->getAuthHeaders('GET', 'offers', '');
		$headers['Content-Length'] = '0';
		return $this->request("/offers", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function getOffer(string $resourceId): string
	{
		$headers = $this->getAuthHeaders('GET', 'offers', $resourceId);
		$headers['Content-Length'] = '0';
		return $this->request("/offers/{$resourceId}", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @param string $json JSON request
	 * @throws GuzzleException
	 */
	public function replaceOffer(string $resourceId, string $json): string
	{
		$headers = $this->getAuthHeaders('PUT', 'offers', $resourceId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/offers/{$resourceId}", "PUT", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @param string $json JSON request
	 * @throws GuzzleException
	 */
	public function queryingOffers(string $json): string
	{
		$headers = $this->getAuthHeaders('POST', 'offers', '');
		$headers['Content-Length'] = strlen($json);
		$headers['Content-Type'] = 'application/query+json';
		$headers['x-ms-documentdb-isquery'] = 'True';
		return $this->request("/offers", "POST", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803949.aspx
	 * @throws GuzzleException
	 */
	public function listPermissions(string $databaseId, string $userId): string
	{
		$headers = $this->getAuthHeaders('GET', 'permissions', $userId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/users/{$userId}/permissions", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803946.aspx
	 * @param string $json JSON request
	 * @throws GuzzleException
	 */
	public function createPermission(string $databaseId, string $userId, string $json): string
	{
		$headers = $this->getAuthHeaders('POST', 'permissions', $userId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/users/{$userId}/permissions", "POST", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803949.aspx
	 * @throws GuzzleException
	 */
	public function getPermission(string $databaseId, string $userId, string $permissionId): string
	{
		$headers = $this->getAuthHeaders('GET', 'permissions', $permissionId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/users/{$userId}/permissions/{$permissionId}", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803949.aspx
	 * @param string $json JSON request
	 * @throws GuzzleException
	 */
	public function replacePermission(string $databaseId, string $userId, string $permissionId, string $json): string
	{
		$headers = $this->getAuthHeaders('PUT', 'permissions', $permissionId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/users/{$userId}/permissions/{$permissionId}", "PUT", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803949.aspx
	 * @throws GuzzleException
	 */
	public function deletePermission(string $databaseId, string $userId, string $permissionId): string
	{
		$headers = $this->getAuthHeaders('DELETE', 'permissions', $permissionId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/users/{$userId}/permissions/{$permissionId}", "DELETE", $headers)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function listStoredProcedures(string $databaseId, string $collectionId): string
	{
		$headers = $this->getAuthHeaders('GET', 'sprocs', $collectionId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/sprocs", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @param string $json Parameters
	 * @throws GuzzleException
	 */
	public function executeStoredProcedure(string $databaseId, string $collectionId, string $storedProcedureId, string $json): string
	{
		$headers = $this->getAuthHeaders('POST', 'sprocs', $storedProcedureId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/sprocs/{$storedProcedureId}", "POST", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803933.aspx
	 * @param string $json JSON of function
	 * @throws GuzzleException
	 */
	public function createStoredProcedure(string $databaseId, string $collectionId, string $json): string
	{
		$headers = $this->getAuthHeaders('POST', 'sprocs', $collectionId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/sprocs", "POST", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @param string $json Parameters
	 * @throws GuzzleException
	 */
	public function replaceStoredProcedure(string $databaseId, string $collectionId, string $storedProcedureId, string $json): string
	{
		$headers = $this->getAuthHeaders('PUT', 'sprocs', $storedProcedureId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/sprocs/{$storedProcedureId}", "PUT", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function deleteStoredProcedure(string $databaseId, string $collectionId, string $storeProcedureId): string
	{
		$headers = $this->getAuthHeaders('DELETE', 'sprocs', $storeProcedureId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/sprocs/{$storeProcedureId}", "DELETE", $headers)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function listUserDefinedFunctions(string $databaseId, string $collectionId): string
	{
		$headers = $this->getAuthHeaders('GET', 'udfs', $collectionId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/udfs", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803933.aspx
	 * @param string $json JSON of function
	 * @throws GuzzleException
	 */
	public function createUserDefinedFunction(string $databaseId, string $collectionId, string $json): string
	{
		$headers = $this->getAuthHeaders('POST', 'udfs', $collectionId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/udfs", "POST", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @param string $json Parameters
	 * @throws GuzzleException
	 */
	public function replaceUserDefinedFunction(string $databaseId, string $collectionId, string $userDefinedFunctionId, string $json): string
	{
		$headers = $this->getAuthHeaders('PUT', 'udfs', $userDefinedFunctionId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/udfs/{$userDefinedFunctionId}", "PUT", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function deleteUserDefinedFunction(string $databaseId, string $collectionId, string $userDefinedFunctionId): string
	{
		$headers = $this->getAuthHeaders('DELETE', 'udfs', $userDefinedFunctionId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/udfs/{$userDefinedFunctionId}", "DELETE", $headers)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function listTriggers(string $databaseId, string $collectionId): string
	{
		$headers = $this->getAuthHeaders('GET', 'triggers', $collectionId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/triggers", "GET", $headers)->getBody()->getContents();
	}

	/**
	 * @link http://msdn.microsoft.com/en-us/library/azure/dn803933.aspx
	 * @param string $json JSON of function
	 * @throws GuzzleException
	 */
	public function createTrigger(string $databaseId, string $collectionId, string $json): string
	{
		$headers = $this->getAuthHeaders('POST', 'triggers', $collectionId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/triggers", "POST", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @param string $json Parameters
	 * @throws GuzzleException
	 */
	public function replaceTrigger(string $databaseId, string $collectionId, string $triggerId, string $json): string
	{
		$headers = $this->getAuthHeaders('PUT', 'triggers', $triggerId);
		$headers['Content-Length'] = strlen($json);
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/triggers/{$triggerId}", "PUT", $headers, $json)->getBody()->getContents();
	}

	/**
	 * @throws GuzzleException
	 */
	public function deleteTrigger(string $databaseId, string $collectionId, string $triggerId): string
	{
		$headers = $this->getAuthHeaders('DELETE', 'triggers', $triggerId);
		$headers['Content-Length'] = '0';
		return $this->request("/dbs/{$databaseId}/colls/{$collectionId}/triggers/{$triggerId}", "DELETE", $headers)->getBody()->getContents();
	}
}
