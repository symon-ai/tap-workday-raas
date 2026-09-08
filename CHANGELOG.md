# Changelog

## 3.5.0
  * Remediate CWE-73 (external control of file name/path): validate the config-supplied `error_file_path` through a centralized `resolve_safe_error_file_path` helper so a crafted path cannot escape the platform-controlled base directory before `open()`. Unsafe paths are skipped (write is non-fatal) and error info still surfaces via markers. [WP-33350]

## 1.0.2
  * Updates ijson version [#13](https://github.com/singer-io/tap-workday-raas/pull/13)
## 1.0.1
  * Added Fix for boolean transformation of `0` and `1` string characters [#10](https://github.com/singer-io/tap-workday-raas/pull/10)
  * Added unit test for boolean evaluation
  * Updated CircleCi Config to resolve python version incompatibility issue

## 1.0.0
  * Bumping to 1.0.0 for GA release

## 0.1.0
  * Adds support for nested objects [#2](https://github.com/singer-io/tap-workday-raas/pull/2)
