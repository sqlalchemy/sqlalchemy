def warn_tox():
    print(
        "\n"
        + "=" * 80
        + "\n\033[1;31m  ⚠️  NOTE: TOX IS DEPRECATED IN THIS PROJECT!  ⚠️"
        "\033[0m\n\033[1;33m  "
        "Please use nox instead for running tests.\033[0m\n" + "=" * 80 + "\n"
    )


if __name__ == "__main__":
    warn_tox()
