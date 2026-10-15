"""Open questions, each answered by picking a drawing rather than a sentence.

An option is either the solved layout or that layout with a few columns
moved by hand, so every alternative is drawn by the same renderer and nothing
has to be imagined from a description. A hand-moved option is a picture of an
answer, not an algorithm: it exists to find out which rule to write.
"""

from dataclasses import dataclass, field

from cases import CASES


@dataclass
class Option:
    label: str
    caption: str
    case: str
    nodes: dict = field(default_factory=dict)


QUESTIONS: list[tuple[str, str, list[Option]]] = []


def ask(name: str, prompt: str, *options: Option):
    for o in options:
        assert o.case in CASES, o.case
    QUESTIONS.append((name, prompt, list(options)))

