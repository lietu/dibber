from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

PriorityBuilds = list[list[str]]


# The properties in dibber.toml should have matching names
class Settings(BaseSettings):
    # User or organization name, used for "user/image" -name generation, optionally leading registry hostname
    docker_user: str

    local_registry: str = ""

    # List of images that should be built beforehand
    priority_builds: PriorityBuilds = []

    model_config = SettingsConfigDict(toml_file="dibber.toml")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (TomlConfigSettingsSource(settings_cls),)


conf = Settings()
