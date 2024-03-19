from pathlib import Path


class PrismException(Exception):
    pass


class PrismASTException(PrismException):

    def __init__(self, call_name: str, attribute: str):
        self.message = f"AST error: `{call_name}` argument does not have `{attribute}` attribute"  # noqa: E501
        super().__init__(self.message)

    def __str__(self):
        return self.message


class ProjectAlreadyExistsException(PrismException):

    def __init__(self, project_dir: Path):
        self.message = f"Project already exists at `{project_dir}`"
        super().__init__(self.message)


class RuntimeException(PrismException):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message


class CompileException(PrismException):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message


class DAGException(PrismException):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message


class ConsoleEventException(PrismException):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message


class ParserException(PrismException):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message


class ReferenceException(PrismException):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message
