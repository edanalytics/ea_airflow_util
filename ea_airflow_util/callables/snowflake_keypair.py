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

    # If both key slots are set, rotation is not allowed
    if key_1 not in (None, "", "null") and key_2 not in (None, "", "null"): #TODO: is there a better way to do this?
        raise RuntimeError("Both RSA_PUBLIC_KEY and RSA_PUBLIC_KEY_2 are set; refusing rotation.")

    # If neither key slot is set, use RSA_PUBLIC_KEY for initial set
    if key_1 in (None, "", "null") and key_2 in (None, "", "null"):
        return "RSA_PUBLIC_KEY", None

    # If only RSA_PUBLIC_KEY is set, rotate to RSA_PUBLIC_KEY_2
    if key_1 not in (None, "", "null"):
        return "RSA_PUBLIC_KEY_2", "RSA_PUBLIC_KEY"

    # If only RSA_PUBLIC_KEY_2 is set, rotate to RSA_PUBLIC_KEY
    return "RSA_PUBLIC_KEY", "RSA_PUBLIC_KEY_2"


def set_user_public_key(
    hook: SnowflakeHook,
    snowflake_user: str,
    new_slot: str,
    public_key_body: str,
    **kwargs
) -> None:
    """
    Set the new public key in the selected slot.

    https://docs.snowflake.com/en/user-guide/key-pair-auth#configuring-key-pair-rotation
    """
    hook.run(f"ALTER USER {snowflake_user} SET {new_slot}='{public_key_body}'")

    logging.info(f"Set `{new_slot}` for Snowflake user `{snowflake_user}`.")


def unset_public_key_slot(
    hook: SnowflakeHook,
    snowflake_user: str,
    slot: Optional[str],
    **kwargs
) -> None:
    """
    Unset a specific public key slot on a Snowflake user.
    """
    if not slot:
        logging.info(f"No old key slot to unset for `{snowflake_user}`.")
        return

    hook.run(f"ALTER USER {snowflake_user} UNSET {slot}")

    logging.info(f"Unset `{slot}` for Snowflake user `{snowflake_user}`.")


def rotate_keypair(
    key_rotator_conn_id: str,
    snowflake_user: str,
    output_dir: str,
    **kwargs
) -> Dict[str, str]:
    """
    Rotate a Snowflake user's keypair.

    - generate new key files on disk
    - set the new public key in Snowflake
    - unset the old key slot

    Failure guardrail:
    - if something fails after setting the new slot but before unsetting the old one,
    attempt to unset the old slot so Snowflake still matches the new key written in /efs
    """
    hook = SnowflakeHook(snowflake_conn_id=key_rotator_conn_id, log_sql=False) #turn off sql log

    private_key_path = os.path.join(output_dir, f"{snowflake_user}.p8")
    public_key_path = os.path.join(output_dir, f"{snowflake_user}.pub")

    # pull public key from generated keypair
    _, _, public_body = generate_keypair_with_openssl(
        output_dir=output_dir,
        snowflake_user=snowflake_user,
    )

    new_slot = None
    old_slot = None
    new_key_set = False

    try:
        desc = hook.get_records(f"DESC USER {snowflake_user}")
        new_slot, old_slot = pick_rotation_slots(desc)

        logging.info(f"Rotation for `{snowflake_user}`: new_slot=`{new_slot}`, old_slot=`{old_slot or 'none'}`.")

        set_user_public_key(
            hook=hook,
            snowflake_user=snowflake_user,
            new_slot=new_slot,
            public_key_body=public_body,
        )
        new_key_set = True # new key was set

        # unset the old slot
        unset_public_key_slot(
            hook=hook,
            snowflake_user=snowflake_user,
            slot=old_slot,
        )

        result = {
            "snowflake_user": snowflake_user,
            "new_slot": new_slot or "",
            "old_slot_unset": old_slot or "",
            "key_file_path": private_key_path,
            "public_key_path": public_key_path,
        }

        logging.info(
            f"Successfully rotated keypair for `{result['snowflake_user']}`: new_slot=`{result['new_slot']}`,"
            f" old_slot_unset=`{result['old_slot_unset'] or 'none'}`,"
            f" private_key=`{result['key_file_path']}`,"
            f" public_key=`{result['public_key_path']}`."
        )

        return result

    # If something fails after setting the new key but before the old one is unset,
    # the user could end up with both key slots filled. Try to clean up by removing
    # the newly added key so the next rotation can run normally.
    except Exception:
        if new_key_set and old_slot:
            logging.exception(f"Rotation failed after setting new slot `{new_slot}` for `{snowflake_user}`. Attempting cleanup.")
            try:
                unset_public_key_slot(
                    hook=hook,
                    snowflake_user=snowflake_user,
                    slot=old_slot,
                )
                logging.info(f"Cleanup succeeded for `{snowflake_user}`: unset old slot `{old_slot}`.")
            except Exception:
                logging.exception(f"Cleanup  failed for `{snowflake_user}`: unable to unset old slot `{old_slot}`.")

        raise

# function to use on initial keypair generation when no keys exist yet (e.g. to make key for key rotator)
def initial_keypair(
    snowflake_user: str,
    output_dir: str,
    **kwargs
) -> Dict[str, str]:
    """
    Generate the initial keypair for a Snowflake user.
    """
    private_key_path = os.path.join(output_dir, f"{snowflake_user}.p8")
    public_key_path = os.path.join(output_dir, f"{snowflake_user}.pub")

    _, _, public_body = generate_keypair_with_openssl(
        output_dir=output_dir,
        snowflake_user=snowflake_user,
    )

    logging.info(f"Generated initial keypair for Snowflake user `{snowflake_user}`.")

    return {
        "snowflake_user": snowflake_user,
        "alter_user_sql": f"ALTER USER {snowflake_user} SET RSA_PUBLIC_KEY='{public_body}';",
        "key_file_path": private_key_path,
        "public_key_path": public_key_path,
    }