import os

import streamlit as st


def get_secret(name: str, default: str = "") -> str:
    value = st.secrets.get(name)
    if value:
        return str(value)
    return os.getenv(name, default)


def has_kis_credentials() -> bool:
    return bool(get_secret("KIS_APP_KEY")) and bool(get_secret("KIS_APP_SECRET"))


def has_kis_account() -> bool:
    return has_kis_credentials() and bool(get_secret("KIS_CANO")) and bool(get_secret("KIS_ACNT_PRDT_CD"))
