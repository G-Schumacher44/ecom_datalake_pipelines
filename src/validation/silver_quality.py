#!/usr/bin/env python3
"""Wrapper for Silver Layer Quality Validation (Refactored).

This file is preserved for backward compatibility. 
The actual logic has been moved to src/validation/silver/.
"""

from src.validation.silver.__main__ import main

if __name__ == "__main__":
    raise SystemExit(main())