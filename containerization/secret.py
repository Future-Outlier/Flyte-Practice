import os
from typing import Tuple

import flytekit
from flytekit import Secret, task, workflow
from flytekit.testing import SecretsManager

secret = Secret(
    group="<SECRET_GROUP>",
    key="<SECRET_KEY>",
    mount_requirement=Secret.MountType.ENV_VAR,
)

SECRET_GROUP = "user-info"
SECRET_NAME = "user_secret"

USERNAME_SECRET = "username"
PASSWORD_SECRET = "password"

@task(
    secret_requests=[
        Secret(group=SECRET_GROUP, key=USERNAME_SECRET),
        Secret(group=SECRET_GROUP, key=PASSWORD_SECRET)
        # Secret(key=PASSWORD_SECRET, group=SECRET_GROUP),
    ]
)
def user_info_task() -> Tuple[str, str]:
    context = flytekit.current_context()
    secret_username = context.secrets.get(SECRET_GROUP, USERNAME_SECRET)
    secret_pwd = context.secrets.get(SECRET_GROUP, PASSWORD_SECRET)
    print(f"{secret_username}={secret_pwd}")
    return secret_username, secret_pwd

@task(secret_requests=[Secret(group=SECRET_GROUP, key=SECRET_NAME)])
def secret_task() -> str:
    context = flytekit.current_context()
    secret_val = context.secrets.get(SECRET_GROUP, SECRET_NAME)
    print(secret_val)
    return secret_val

# secret_task()