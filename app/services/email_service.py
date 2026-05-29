"""Email notification service.

SMTP settings are read from existing environment variables only. This module
does not create or edit .env files.
"""

from __future__ import annotations

import os
import smtplib
import traceback
from dataclasses import dataclass
from email.message import EmailMessage

import app.config  # noqa: F401 - loads existing .env values into os.environ


@dataclass
class EmailConfigValue:
    value: str
    source: str


@dataclass
class EmailConfig:
    host: EmailConfigValue
    port: EmailConfigValue
    user: EmailConfigValue
    password: EmailConfigValue
    from_email: EmailConfigValue

    @property
    def mode(self) -> str:
        return "SSL" if str(self.port.value).strip() == "465" else "STARTTLS"


def _first_env(names: list[str]) -> EmailConfigValue:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return EmailConfigValue(value=value, source=name)
    return EmailConfigValue(value="", source="missing")


def resolve_email_config() -> EmailConfig:
    user = _first_env([
        "SMTP_USER",
        "GMAIL_ADDRESS",
        "EMAIL_USER",
        "MAIL_USERNAME",
        "GMAIL_USER",
    ])
    password = _first_env([
        "SMTP_PASSWORD",
        "GMAIL_APP_PASSWORD",
        "EMAIL_PASSWORD",
        "MAIL_PASSWORD",
        "GMAIL_PASSWORD",
    ])
    from_email = _first_env([
        "ALERT_FROM_EMAIL",
        "GMAIL_ADDRESS",
        "EMAIL_FROM",
        "MAIL_FROM",
        "SENDER_EMAIL",
    ])
    if not from_email.value and user.value:
        from_email = EmailConfigValue(user.value, user.source)

    return EmailConfig(
        host=_first_env(["SMTP_HOST", "EMAIL_HOST", "MAIL_HOST"]) if _first_env(["SMTP_HOST", "EMAIL_HOST", "MAIL_HOST"]).value else EmailConfigValue("smtp.gmail.com", "default"),
        port=_first_env(["SMTP_PORT", "EMAIL_PORT", "MAIL_PORT"]) if _first_env(["SMTP_PORT", "EMAIL_PORT", "MAIL_PORT"]).value else EmailConfigValue("587", "default"),
        user=user,
        password=password,
        from_email=from_email,
    )


def email_config_debug() -> dict:
    config = resolve_email_config()
    return {
        "host": config.host.value,
        "port": int(config.port.value) if str(config.port.value).isdigit() else config.port.value,
        "mode": config.mode,
        "user_loaded": bool(config.user.value),
        "password_loaded": bool(config.password.value),
        "from_email": config.from_email.value,
        "user_source": config.user.source,
        "password_source": config.password.source,
        "from_source": config.from_email.source,
    }


def _log_config(config: EmailConfig) -> None:
    print(f"[EMAIL CONFIG] host={config.host.value}")
    print(f"[EMAIL CONFIG] port={config.port.value}")
    print(f"[EMAIL CONFIG] mode={config.mode}")
    print(f"[EMAIL CONFIG] user source={config.user.source} loaded={'yes' if config.user.value else 'no'}")
    print(f"[EMAIL CONFIG] password source={config.password.source} loaded={'yes' if config.password.value else 'no'}")
    print(f"[EMAIL CONFIG] from source={config.from_email.source} loaded={'yes' if config.from_email.value else 'no'}")


def _validate_config(config: EmailConfig) -> tuple[bool, str]:
    if not config.user.value or not config.password.value:
        return False, "Missing email config: GMAIL_ADDRESS/GMAIL_APP_PASSWORD or SMTP_USER/SMTP_PASSWORD"
    if not config.from_email.value:
        return False, "Missing email config: ALERT_FROM_EMAIL/GMAIL_ADDRESS or SMTP_USER"
    try:
        int(config.port.value)
    except (TypeError, ValueError):
        return False, f"Invalid email config: port must be a number, got {config.port.value!r}"
    return True, ""


def _send_email(to_email: str, subject: str, body: str, log_label: str) -> tuple[bool, str, str]:
    config = resolve_email_config()
    _log_config(config)
    to_email = (to_email or "").strip()
    if not to_email:
        return False, "recipient email is required", "validate"

    valid, error = _validate_config(config)
    if not valid:
        print(f"[{log_label}] failed: {error}")
        return False, error, "config"

    msg = EmailMessage()
    msg["From"] = config.from_email.value.strip()
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body or "")

    server = None
    smtp_port = int(config.port.value)
    try:
        print("[EMAIL SMTP] connecting...")
        if smtp_port == 465:
            print("[EMAIL SMTP] mode=SSL port=465")
            server = smtplib.SMTP_SSL(config.host.value, smtp_port, timeout=30)
            server.set_debuglevel(1)
            print("[EMAIL SMTP] connected")
            stage = "login"
            server.login(config.user.value.strip(), config.password.value.strip())
            print("[EMAIL SMTP] login ok")
        else:
            print(f"[EMAIL SMTP] mode=STARTTLS port={smtp_port}")
            server = smtplib.SMTP(config.host.value, smtp_port, timeout=30)
            server.set_debuglevel(1)
            print("[EMAIL SMTP] connected")
            stage = "ehlo"
            server.ehlo()
            print("[EMAIL SMTP] ehlo ok")
            stage = "starttls"
            server.starttls()
            print("[EMAIL SMTP] starttls ok")
            stage = "ehlo"
            server.ehlo()
            print("[EMAIL SMTP] ehlo ok")
            stage = "login"
            server.login(config.user.value.strip(), config.password.value.strip())
            print("[EMAIL SMTP] login ok")

        stage = "send_message"
        server.send_message(msg)
        print("[EMAIL SMTP] send_message ok")
        return True, "sent", "sent"
    except Exception as exc:
        failed_stage = locals().get("stage", "connect")
        print(f"[EMAIL SMTP] failed at {failed_stage}: {exc}")
        print(traceback.format_exc())
        return False, str(exc), failed_stage
    finally:
        if server is not None:
            try:
                server.quit()
            except Exception:
                pass


def send_test_email(to_email: str) -> tuple[bool, str, str]:
    print("[EMAIL TEST] API called")
    print(f"[EMAIL TEST] target email={to_email}")
    ok, detail, stage = _send_email(
        to_email,
        "[MetricsPulse Test] Email alert is working",
        (
            "This is a test email from MetricsPulse.\n"
            "If you received this email, Gmail SMTP is configured correctly."
        ),
        "EMAIL TEST",
    )
    if ok:
        print("[EMAIL TEST] sent successfully")
    else:
        print(f"[EMAIL TEST] failed: {detail}")
    return ok, detail, stage


def send_email_alert(to_email: str, subject: str, body: str) -> tuple[bool, str]:
    ok, detail, _stage = _send_email(to_email, subject, body, "EMAIL ALERT")
    if ok:
        print(f"[EMAIL ALERT] sent to {to_email}")
    else:
        print(f"[EMAIL ALERT] failed: {detail}")
    return ok, detail
