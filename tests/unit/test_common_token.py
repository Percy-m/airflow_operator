from custom_operator.common.token import StaticTokenProvider, mask_token


def test_mask_token_shows_first_five_characters_only():
    assert mask_token("abcdefghij") == "abcde***"
    assert mask_token("abcde") == "abcde***"
    assert mask_token("abcd") == "***"
    assert mask_token("") == "<empty>"


def test_static_token_provider_returns_fixed_token():
    provider = StaticTokenProvider("static-token")

    assert provider.get_token() == "static-token"
    assert provider.refresh_token() == "static-token"
