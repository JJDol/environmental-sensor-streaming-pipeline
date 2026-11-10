import math
from datetime import datetime, timezone

# Demonstration thresholds, not environmental safety standards.
TRIGGER_TEMPERATURE_C = 30.0
CLEAR_TEMPERATURE_C = 28.0

def evaluate_temperature_alert(event: dict, alerts) -> None:
    alert_id = f"{event['sensor_id']}:high_temperature"
    current = alerts.find_one({"_id": alert_id})
    observed_at = datetime.fromisoformat(event["observed_at"])

    # A historical replay must not move the current alert state backwards.
    if current and observed_at <= datetime.fromisoformat(current["last_observed_at"]):
        return

    temperature = event["measurements"]["temperature_c"]
    if temperature is not None and (
        isinstance(temperature, bool)
        or not isinstance(temperature, (int, float))
        or not math.isfinite(temperature)
    ):
        print(f"WARNING: {event['event_id']} has invalid temperature; alert unchanged")
        return

    active = current is not None and current["status"] == "active"
    transition = None
    changes = {
        "last_event_id": event["event_id"],
        "last_observed_at": event["observed_at"],
        "last_temperature_c": temperature,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if temperature is None:
        if not current:
            return
    elif not active and temperature > TRIGGER_TEMPERATURE_C:
        changes.update({
            "status": "active",
            "opened_at": event["observed_at"],
            "resolved_at": None,
        })
        transition = "opened"
    elif active and temperature < CLEAR_TEMPERATURE_C:
        changes.update({
            "status": "resolved",
            "resolved_at": event["observed_at"],
        })
        transition = "resolved"
    elif not current:
        return

    alerts.update_one(
        {"_id": alert_id},
        {
            "$set": changes,
            "$setOnInsert": {
                "sensor_id": event["sensor_id"],
                "rule": "high_temperature",
                "trigger_threshold": TRIGGER_TEMPERATURE_C,
                "clear_threshold": CLEAR_TEMPERATURE_C,
            },
        },
        upsert=True,
    )
    if transition:
        print(
            f"NOTIFICATION: High temperature alert {transition} for "
            f"{event['sensor_id']} at {temperature} C "
            f"(event {event['event_id']})"
        )
