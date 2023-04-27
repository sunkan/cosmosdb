<?php

namespace Jupitern\CosmosDb;

class QueryBuilder
{
	private CosmosDbCollection $collection;
	private ?string $partitionKey = null;
	private ?string $partitionValue = null;
	private string $queryString = "";
	private string $fields = "";
	private string $from = "c";
	private string $join = "";
	private string $where = "";
	private ?string $order = null;
	private ?string $limit = null;
	private array $triggers = [];
	private array $params = [];
	private ?array $response = null;
	private bool $multipleResults = false;

	public static function instance(): static
	{
		return new static();
	}

	public function setCollection(CosmosDbCollection $collection): static
	{
		$this->collection = $collection;
		return $this;
	}

	/**
	 * @param array|string $fields
	 */
	public function select(array|string $fields): static
	{
		if (is_array($fields)) {
			$fields = 'c["' . implode('"], c["', $fields) . '"]';
		}
		$this->fields = $fields;
		return $this;
	}

	public function from(string $from): static
	{
		$this->from = $from;
		return $this;
	}

	public function join(string $join): static
	{
		$this->join .= " {$join} ";
		return $this;
	}

	public function where(string $where): static
	{
		if (empty($where)) {
			return $this;
		}
		$this->where .= !empty($this->where) ? " and {$where} " : "{$where}";

		return $this;
	}

	/**
	 * @param scalar $value
	 */
	public function whereStartsWith(string $field, mixed $value): static
	{
		return $this->where("STARTSWITH($field, '{$value}')");
	}

	/**
	 * @param scalar $value
	 */
	public function whereEndsWith(string $field, mixed $value): static
	{
		return $this->where("ENDSWITH($field, '{$value}')");
	}

	/**
	 * @param scalar $value
	 */
	public function whereContains(string $field, mixed $value): static
	{
		return $this->where("CONTAINS($field, '{$value}'");
	}

	/**
	 * @param array $values
	 */
	public function whereIn(string $field, array $values): static
	{
		if (empty($values)) {
			return $this;
		}

		return $this->where("$field IN('" . implode("', '", $values) . "')");
	}

	/**
	 * @param array $values
	 */
	public function whereNotIn(string $field, array $values): static
	{
		if (empty($values)) {
			return $this;
		}

		return $this->where("$field NOT IN('" . implode("', '", $values) . "')");
	}

	public function order(string $order): static
	{
		$this->order = $order;
		return $this;
	}

	/**
	 * @param int $limit
	 */
	public function limit(int $limit): static
	{
		$this->limit = $limit;
		return $this;
	}

	/**
	 * @param array $params
	 */
	public function params(array $params): static
	{
		$this->params = $params;
		return $this;
	}

	public function findAll(bool $isCrossPartition = false): static
	{
		$this->response = null;
		$this->multipleResults = true;

		$partitionValue = $this->partitionValue != null ? $this->partitionValue : null;

		$limit = $this->limit != null ? "top " . (int)$this->limit : "";
		$fields = !empty($this->fields) ? $this->fields : '*';
		$where = $this->where != "" ? "where {$this->where}" : "";
		$order = $this->order != "" ? "order by {$this->order}" : "";

		$query = "SELECT {$limit} {$fields} FROM {$this->from} {$this->join} {$where} {$order}";

		$this->response = $this->collection->query($query, $this->params, $isCrossPartition, $partitionValue);

		return $this;
	}

	public function find(bool $isCrossPartition = false): static
	{
		$this->response = null;
		$this->multipleResults = false;

		$partitionValue = $this->partitionValue != null ? $this->partitionValue : null;

		$fields = !empty($this->fields) ? $this->fields : '*';
		$where = $this->where != "" ? "where {$this->where}" : "";
		$order = $this->order != "" ? "order by {$this->order}" : "";

		$query = "SELECT top 1 {$fields} FROM {$this->from} {$this->join} {$where} {$order}";

		$this->response = $this->collection->query($query, $this->params, $isCrossPartition, $partitionValue);

		return $this;
	}

	/* insert / update */

	public function setPartitionKey(string $fieldName): static
	{
		$this->partitionKey = $fieldName;

		return $this;
	}

	public function getPartitionKey(): ?string
	{
		return $this->partitionKey;
	}

	public function setPartitionValue(?string $fieldName): static
	{
		$this->partitionValue = $fieldName;

		return $this;
	}

	public function getPartitionValue(): ?string
	{
		return $this->partitionValue;
	}

	public function setQueryString(string $string): static
	{
		$this->queryString .= $string;
		return $this;
	}

	public function getQueryString(): ?string
	{
		return $this->queryString;
	}

	public function isNested(string $partitionKey): bool|static
	{
		# strip any slashes from the beginning
		# and end of the partition key
		$partitionKey = trim($partitionKey, '/');

		# if the partition key contains slashes, the user
		# is referencing a nested value, so we should search for it
		return str_contains($partitionKey, '/');
	}

	/**
	 * Find and set the partition value
	 *
	 * @param object document
	 * @param bool if true, return property structure formatted for use in Azure query string
	 * @return string partition value
	 */
	public function findPartitionValue(object $document, bool $toString = false)
	{
		# if the partition key contains slashes, the user
		# is referencing a nested value, so we should find it
		if ($this->isNested($this->partitionKey)) {

			# explode the key into its properties
			$properties = array_values(array_filter(explode("/", $this->partitionKey)));

			# return the property structure
			# formatted as a cosmos query string
			if ($toString) {

				foreach ($properties as $p) {
					$this->setQueryString($p);
				}

				return $this->queryString;
			}
			# otherwise, iterate through the document
			# and find the value of the property key
			else {

				foreach ($properties as $p) {
					$document = (object)$document->{$p};
				}

				return $document->scalar;
			}
		}
		# otherwise, assume the key is in the root of the
		# document and return the value of the property key
		else {
			return $document->{$this->partitionKey};
		}
	}

	/**
	 * @throws \Exception
	 */
	public function save(mixed $document): ?string
	{
		$document = (object)$document;

		$rid = is_object($document) && isset($document->_rid) ? $document->_rid : null;
		$partitionValue = $this->partitionKey != null ? $this->findPartitionValue($document) : null;
		$document = json_encode($document);

		$result = $rid ?
			$this->collection->replaceDocument($rid, $document, $partitionValue, $this->triggersAsHeaders("replace")) :
			$this->collection->createDocument($document, $partitionValue, $this->triggersAsHeaders("create"));
		$resultObj = json_decode($result);

		if (isset($resultObj->code) && isset($resultObj->message)) {
			throw new \Exception("$resultObj->code : $resultObj->message");
		}

		return $resultObj->_rid ?? null;
	}

	/* delete */

	public function delete(bool $isCrossPartition = false): bool
	{
		$this->response = null;

		$select = $this->fields != "" ?
			$this->fields : "c._rid" . ($this->partitionKey != null ? ", c." . $this->partitionKey : "");
		$document = $this->select($select)->find($isCrossPartition)->toObject();

		if ($document) {
			$partitionValue = $this->partitionKey != null ? $this->findPartitionValue($document) : null;
			$this->response = $this->collection->deleteDocument($document->_rid, $partitionValue, $this->triggersAsHeaders("delete"));

			return true;
		}

		return false;
	}

	public function deleteAll(bool $isCrossPartition = false): bool
	{
		$this->response = null;

		$select = $this->fields != "" ?
			$this->fields : "c._rid" . ($this->partitionKey != null ? ", c." . $this->partitionKey : "");
		$response = [];
		foreach ((array)$this->select($select)->findAll($isCrossPartition)->toObject() as $document) {
			$partitionValue = $this->partitionKey != null ? $this->findPartitionValue($document) : null;
			$response[] = $this->collection->deleteDocument($document->_rid, $partitionValue, $this->triggersAsHeaders("delete"));
		}

		$this->response = $response;
		return true;
	}

	/* triggers */

	/**
	 * @throws \Exception
	 */
	public function addTrigger(string $operation, string $type, string $id): static
	{
		$operation = \strtolower($operation);
		if (!\in_array($operation, ["all", "create", "delete", "replace"])) {
			throw new \Exception("Trigger: Invalid operation \"{$operation}\"");
		}

		$type = \strtolower($type);
		if (!\in_array($type, ["post", "pre"])) {
			throw new \Exception("Trigger: Invalid type \"{$type}\"");
		}

		if (!isset($this->triggers[$operation][$type])) {
			$this->triggers[$operation][$type] = [];
		}

		$this->triggers[$operation][$type][] = $id;
		return $this;
	}

	/**
	 * @return array
	 */
	protected function triggersAsHeaders(string $operation): array
	{
		$headers = [];

		// Add headers for the current operation type at $operation (create|delete!replace)
		if (isset($this->triggers[$operation])) {
			foreach ($this->triggers[$operation] as $name => $ids) {
				$ids = \is_array($ids) ? $ids : [$ids];
				$headers["x-ms-documentdb-{$name}-trigger-include"] = \implode(",", $ids);
			}
		}

		// Add headers for the special "all" operations type that should always run
		if (isset($this->triggers["all"])) {
			foreach ($this->triggers["all"] as $name => $ids) {
				$headerKey = "x-ms-documentdb-{$name}-trigger-include";
				$ids = \implode(",", \is_array($ids) ? $ids : [$ids]);
				$headers[$headerKey] = isset($headers[$headerKey]) ? $headers[$headerKey] .= "," . $ids : $headers[$headerKey] = $ids;
			}
		}

		return $headers;
	}

	/* helpers */

	public function toJson(): string
	{
		/*
		 * If the CosmosDB result set contains many documents, CosmosDB might apply pagination. If this is detected,
		 * all pages are requested one by one, until all results are loaded. These individual responses are contained
		 * in $this->response. If no pagination is applied, $this->response is an array containing a single response.
		 *
		 * $results holds the documents returned by each of the responses.
		 */
		$results = [
			'_rid' => '',
			'_count' => 0,
			'Documents' => []
		];
		foreach ($this->response as $response) {
			$res = json_decode($response);
			$results['_rid'] = $res->_rid;
			$results['_count'] = $results['_count'] + $res->_count;
			$docs = $res->Documents ?? [];
			$results['Documents'] = array_merge($results['Documents'], $docs);
		}
		return json_encode($results);
	}

	/**
	 * @param $arrayKey
	 * @return mixed
	 */
	public function toObject($arrayKey = null)
	{
		/*
		 * If the CosmosDB result set contains many documents, CosmosDB might apply pagination. If this is detected,
		 * all pages are requested one by one, until all results are loaded. These individual responses are contained
		 * in $this->response. If no pagination is applied, $this->response is an array containing a single response.
		 *
		 * $results holds the documents returned by each of the responses.
		 */
		$results = [];
		foreach ((array)$this->response as $response) {
			$res = json_decode($response);
			if (isset($res->Documents)) {
				array_push($results, ...$res->Documents);
			}
			else {
				$results[] = $res;
			}
		}

		if ($this->multipleResults && $arrayKey != null) {
			$results = array_combine(array_column($results, $arrayKey), $results);
		}

		return $this->multipleResults ? $results : ($results[0] ?? null);
	}

	/**
	 * @param $arrayKey
	 * @return array|mixed
	 */
	public function toArray($arrayKey = null): array|null
	{
		$results = (array)$this->toObject($arrayKey);

		if ($this->multipleResults) {
			array_walk($results, function (&$value) {
				$value = (array)$value;
			});
		}

		return $this->multipleResults ? $results : ((array)$results ?? null);
	}

	/**
	 * @return mixed
	 */
	public function getValue(string $fieldName, mixed $default = null): mixed
	{
		return ($this->toObject())->{$fieldName} ?? $default;
	}
}
