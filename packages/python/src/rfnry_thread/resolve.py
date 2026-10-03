from collections.abc import Sequence

from rfnry_thread.models import Edit, Event, Message, Retract


def resolve(events: Sequence[Event]) -> list[Event]:
    ordered = sorted(events, key=lambda event: event.seq)
    edits: dict[str, Edit] = {}
    retracted: set[str] = set()
    for event in ordered:
        if isinstance(event, Edit):
            edits[event.target] = event
        elif isinstance(event, Retract):
            retracted.add(event.target)
    view: list[Event] = []
    for event in ordered:
        if isinstance(event, Edit | Retract) or event.id in retracted:
            continue
        if isinstance(event, Message) and event.id in edits:
            event = event.model_copy(update={"content": edits[event.id].content})
        view.append(event)
    return view


def last_message(events: Sequence[Event]) -> Message | None:
    return next((event for event in reversed(resolve(events)) if isinstance(event, Message)), None)
