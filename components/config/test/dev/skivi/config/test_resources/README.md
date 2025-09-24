# Test Resources

This directory contains EDN test fixture files used by the config component tests.

## Directory Structure

These files are placed within the namespace hierarchy (`test/dev/skivi/config/test_resources/`) following Polylith best practices for test resources (see [polylith#276](https://github.com/polyfy/polylith/issues/276)). This placement ensures they are on the classpath while avoiding Polylith Warning 205 for non-top namespaces.

## Files

- `valid-standalone.edn` - Valid standalone config (root level)
- `valid-embedded.edn` - Valid config with `:skivi` section (embedded mode)
- `embedded-no-skivi.edn` - Embedded config without `:skivi` section (for error testing)
- `minimal-config.edn` - Minimal valid config (required sections only)
- `config-with-optional.edn` - Full config with all optional sections
- `invalid-config.edn` - Invalid config for schema validation testing
- `profile-config.edn` - Config with profile-based overrides (dev/test/prod)
- `aero-features.edn` - Config demonstrating aero readers (#env, #or, #long, #boolean)
