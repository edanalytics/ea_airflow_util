import logging
import os
import subprocess

from typing import Optional, Tuple, Dict

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def generate_keypair_with_openssl(
    output_dir: str,
    snowflake_user: str,
    **kwargs
) -> Tuple[str, str, str]:
    """
    Generate a Snowflake RSA keypair using openssl.
    """
    os.makedirs(output_dir, exist_ok=True)

    private_path = os.path.join(output_dir, f"{snowflake_user}.p8")
    public_path  = os.path.join(output_dir, f"{snowflake_user}.pub")

    # https://docs.snowflake.com/en/user-guide/key-pair-auth
    subprocess.run(
        f"openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out {private_path} -nocrypt",
        shell=True,
        check=True,
        executable="/bin/bash",
    )

    subprocess.run(
        f"openssl rsa -in {private_path} -pubout -out {public_path}",
        shell=True,
        check=True,
        executable="/bin/bash",
    )

    with open(private_path, "r", encoding="utf-8") as reader:
        private_pem = reader.read()

    with open(public_path, "r", encoding="utf-8") as reader:
        public_pem = reader.read()

    public_body = (
        public_pem.replace("-----BEGIN PUBLIC KEY-----", "")
        .replace("-----END PUBLIC KEY-----", "")
        .replace("\n", "")
        .replace("\r", "")
        .strip()
    )

    logging.info(f"Generated keypair for `{snowflake_user}` in `{output_dir}`.")

    return private_pem, public_pem, public_body


def pick_rotation_slots(desc_user_rows: list[tuple]) -> Tuple[str, Optional[str]]:
    """
    Determine which Snowflake public key slot to use for rotation.
    """
    user_att  = {row[0]: row[1] for row in desc_user_rows}
    key_1 = user_att.get("RSA_PUBLIC_KEY")
    key_2 = user_att.get("RSA_PUBLIC_KEY_2")

    if key_1 and key_2:
        raise RuntimeError("Both RSA_PUBLIC_KEY and RSA_PUBLIC_KEY_2 are set; refusing rotation.")

    if (not key_1) and (not key_2):
        return "RSA_PUBLIC_KEY", None

    if key_1 and (not key_2):
        return "RSA_PUBLIC_KEY_2", "RSA_PUBLIC_KEY"

    return "RSA_PUBLIC_KEY", "RSA_PUBLIC_KEY_2"


def set_user_public_key(
    key_rotator_conn_id: str,
    snowflake_user: str,
    public_key_body: str,
    **kwargs 
) -> Dict[str, str]:
    """
    Set the next available public key on a Snowflake user.
    """
    hook = SnowflakeHook(snowflake_conn_id=key_rotator_conn_id)

    desc = hook.get_records(f"DESC USER {snowflake_user}")
    new_slot, old_slot = pick_rotation_slots(desc)

    hook.run(f"ALTER USER {snowflake_user} SET {new_slot}='{public_key_body}'")

    logging.info(f"Set `{new_slot}` for Snowflake user `{snowflake_user}`.")

    return {
        "new_slot": new_slot,
        "old_slot": old_slot or "",
    }


def unset_old_public_key(
    key_rotator_conn_id: str,
    snowflake_user: str,
    old_slot: Optional[str],
    **kwargs 
) -> None:
    """
    Unset the old public key slot after a successful rotation.
    """
    if not old_slot:
        logging.info(f"No old key slot to unset for `{snowflake_user}`.")
        return

    hook = SnowflakeHook(snowflake_conn_id=key_rotator_conn_id)
    hook.run(f"ALTER USER {snowflake_user} UNSET {old_slot}")

    logging.info(f"Unset `{old_slot}` for Snowflake user `{snowflake_user}`.")


def test_current_user(
    conn_id: str,
    expected_user: str,
    **kwargs 
) -> None:
    """
    Test that a Snowflake connection authenticates as the expected user.
    """
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    actual_user = hook.get_first("SELECT CURRENT_USER()")[0]

    if actual_user.upper() != expected_user.upper():
        raise RuntimeError(f"Test failed: expected `{expected_user}`, got `{actual_user}`.")

    logging.info(f"Connection `{conn_id}` authenticated successfully as `{actual_user}`.")


def rotate_keypair(
    key_rotator_conn_id: str,
    snowflake_user: str,
    output_dir: str,
    test_conn_id: Optional[str] = None,
    do_test: bool = True,
    **kwargs 
) -> Dict[str, str]:
    """
    Rotate a Snowflake user's keypair.

    - generate new key files on disk
    - set the new public key in Snowflake
    - test the updated connection if configured
    - unset the old key slot
    """

    # pull public key from generated keypair
    _, _, public_body = generate_keypair_with_openssl(
        output_dir=output_dir,
        snowflake_user=snowflake_user,
    )

    slots = set_user_public_key(
        key_rotator_conn_id=key_rotator_conn_id,
        snowflake_user=snowflake_user,
        public_key_body=public_body,
    )

    if do_test and test_conn_id:
        test_current_user(
            conn_id=test_conn_id,
            expected_user=snowflake_user,
        )

    unset_old_public_key(
        key_rotator_conn_id=key_rotator_conn_id,
        snowflake_user=snowflake_user,
        old_slot=slots.get("old_slot") or None,
    )

    return {
        "snowflake_user": snowflake_user,
        "new_slot": slots.get("new_slot", ""),
        "old_slot_unset": slots.get("old_slot", ""),
        "key_file_path": os.path.join(output_dir, f"{snowflake_user}.p8"),
        "public_key_path": os.path.join(output_dir, f"{snowflake_user}.pub"),
    }


def initial_keypair(
    snowflake_user: str,
    output_dir: str,
    **kwargs 
) -> Dict[str, str]:
    """
    Generate the initial keypair for a Snowflake user.
    """
    _, _, public_body = generate_keypair_with_openssl(
        output_dir=output_dir,
        snowflake_user=snowflake_user,
    )

    logging.info(f"Generated initial keypair for Snowflake user `{snowflake_user}`.")

    return {
        "snowflake_user": snowflake_user,
        "alter_user_sql": f"ALTER USER {snowflake_user} SET RSA_PUBLIC_KEY='{public_body}';",
        "key_file_path": os.path.join(output_dir, f"{snowflake_user}.p8"),
        "public_key_path": os.path.join(output_dir, f"{snowflake_user}.pub"),
    }