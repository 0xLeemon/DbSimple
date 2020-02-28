# Changes against the original DbSimple

## Release 3.0 [2020-02-28]

### Removed
- The only `mysqli` driver supported (does anyone use something else?);
- Dropped CACHE support (I believe this was implemented on mismatched abstraction level);
- Dropped BLOB_OBJ support (mysqli gives a chance to say "temporarily");

### Added
- New placeholder `?o` (ORDER BY)

```php
$db->select("SELECT * FROM ?_something ORDER BY ?o", ['p1' => "ASC", 'p2' => "DESC"]);
```

### Changed
- Hash-result returns a pair instead of assoc-array if that array consists of the only column

```
SELECT `integer` AS ARRAY_KEY, test.* FROM test
  -- 0 ms; returned 59 row(s)

Array
(
    [872] => Array
        (
            [id] => 1
            [integer] => 872
            [string] => io5ZKI7Rd3xzjZ9vjUh3
            [ctime] => 2020-02-23 21:48:37
        )

    [580] => Array
        (
            [id] => 2
            [integer] => 580
            [string] => n4mNHIKZJYEML5dwPR38
            [ctime] => 2020-02-23 21:49:20
        )
    ...
)


SELECT `integer` AS ARRAY_KEY, ctime FROM test
  -- 0 ms; returned 59 row(s)

Array
(
    [872] => 2020-02-23 21:48:37
    [580] => 2020-02-23 21:49:20
    ...
)
```
