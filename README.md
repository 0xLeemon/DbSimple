# DbSimple v3
Simplest but powerful PHP interface to work with MySQL

This is a fork of quite popular, but likely abandoned [DbSimple by DkLab](https://github.com/DmitryKoterov/dbsimple), rewritten for PHP 5.6+/7.0+, mysqli, PSR-4 autoloader; keeping almost 100% compatibility with original interface.

In most cases the only change you need is to replace underscope with backslash in the `connect()` call:

```php
$db = DbSimple_Generic::connect($dsn); // v2
$db = DbSimple\Generic::connect($dsn); // v3
```

Hope you have a PSR-0/PSR-4 compatible autoloader, in other mystical cases you have to include `DbSimple/Generic.php` and `DbSimple/Mysqli.php` explicitly.

Please read the [ChangeLog](CHANGELOG.md) for more details.
