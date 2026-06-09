"""Ray compatibility wrapper for static X-Auth-Token helpers."""

from custom_operator.common.token import StaticTokenProvider, mask_token

__all__ = ["StaticTokenProvider", "mask_token"]
