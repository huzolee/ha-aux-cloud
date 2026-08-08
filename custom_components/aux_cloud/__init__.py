"""Aux Cloud integration for Home Assistant."""

import asyncio
import base64
import json
import time
from datetime import timedelta

import voluptuous as vol
from homeassistant.config_entries import SOURCE_IMPORT, ConfigEntry
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD, CONF_REGION
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.typing import ConfigType
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api.aux_cloud import AuxCloudAPI
from .const import (
    _LOGGER,
    DOMAIN,
    DATA_AUX_CLOUD_CONFIG,
    PLATFORMS,
    CONF_SELECTED_DEVICES,
    MAX_FAILED_POLLS,
)
from .util import DeviceStateHelper

MIN_TIME_BETWEEN_UPDATES = timedelta(seconds=60)

# Schema to include email and password (device selection is handled in config flow)
CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_EMAIL): cv.string,
                vol.Required(CONF_PASSWORD): cv.string,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """
    AUX Cloud setup for configuration.yaml import.
    This is mainly kept for backward compatibility.
    UI configuration is recommended for better security.
    """
    if DOMAIN not in config:
        return True

    hass.data[DATA_AUX_CLOUD_CONFIG] = config.get(DOMAIN, {})

    if (
        not hass.config_entries.async_entries(DOMAIN)
        and hass.data[DATA_AUX_CLOUD_CONFIG]
    ):
        # Import from configuration.yaml if no config entry exists
        hass.async_create_task(
            hass.config_entries.flow.async_init(
                DOMAIN, context={"source": SOURCE_IMPORT}, data=config[DOMAIN]
            )
        )

        # Log a message about UI configuration being preferred
        _LOGGER.info(
            "AUX Cloud configured via configuration.yaml. For better security, "
            "it is recommended to configure this integration through the UI where "
            "credentials are stored encrypted."
        )

    return True


class AuxCloudCoordinator(DataUpdateCoordinator):
    """DataUpdateCoordinator for AUX Cloud."""

    def __init__(
        self,
        hass: HomeAssistant,
        api: AuxCloudAPI,
        email: str,
        password: str,
        selected_device_ids: list,
    ):
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name="AUX Cloud Coordinator",
            update_interval=MIN_TIME_BETWEEN_UPDATES,
        )
        self.api = api
        self.email = email
        self.password = password
        self.selected_device_ids = selected_device_ids
        self.devices = []
        self._device_state_helpers: dict[str, DeviceStateHelper] = {}

        # V2 AUX heat pumps expose their full state through the
        # devpush WebSocket channel instead of DNA.KeyValueControl GET.
        self._ws_params: dict[str, dict] = {}
        self._ws_started = False

    def get_device_by_endpoint_id(self, endpoint_id: str):
        """Get a device by its endpoint ID."""
        return next(
            (
                device
                for device in self.data.get("devices", [])
                if device.get("endpointId") == endpoint_id
            ),
            None,
        )

    def get_state_helper(self, endpoint_id: str, initial_params: dict) -> DeviceStateHelper:
        """Get or create a shared state helper for a single physical device."""
        helper = self._device_state_helpers.get(endpoint_id)
        if helper is None:
            helper = DeviceStateHelper(initial_params, MAX_FAILED_POLLS)
            self._device_state_helpers[endpoint_id] = helper
        return helper

    def _apply_websocket_params(self, endpoint_id: str, params: dict) -> None:
        """Merge parameters received from devpush into coordinator state."""
        if not endpoint_id or not isinstance(params, dict):
            return

        cache = self._ws_params.setdefault(endpoint_id, {})
        cache.update(params)

        updated = False

        for device in self.devices:
            if device.get("endpointId") != endpoint_id:
                continue

            device.setdefault("params", {}).update(params)
            device["last_updated"] = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime()
            )
            updated = True

        if updated:
            _LOGGER.debug(
                "Applied AUX WebSocket update for %s: %s",
                endpoint_id,
                params,
            )
            self.async_set_updated_data({"devices": self.devices})

    async def _handle_websocket_message(self, message: dict) -> None:
        """Handle AUX Cloud devpush WebSocket messages."""
        if message.get("topic") != "devpush":
            return

        msgtype = message.get("msgtype")
        data = message.get("data") or {}

        # Initial subscription acknowledgement contains a full device snapshot.
        if msgtype == "subk":
            for item in data.get("devList", []):
                if item.get("status") != 0:
                    continue

                endpoint_id = item.get("endpointId")
                params = item.get("data") or {}

                if endpoint_id and isinstance(params, dict):
                    _LOGGER.info(
                        "Received AUX V2 WebSocket snapshot for %s",
                        endpoint_id,
                    )
                    self._apply_websocket_params(endpoint_id, params)

            return

        # Subsequent state changes arrive as Base64 encoded JSON.
        if msgtype == "push":
            endpoint_id = data.get("endpointId")
            encoded = data.get("data")

            if not endpoint_id or not encoded:
                return

            try:
                decoded = base64.b64decode(encoded).decode("utf-8")
                params = json.loads(decoded)
            except Exception as exc:
                _LOGGER.warning(
                    "Unable to decode AUX WebSocket push for %s: %s",
                    endpoint_id,
                    exc,
                )
                return

            # did/pid/ver/datatype are metadata, but keeping them is harmless.
            _LOGGER.debug(
                "Received AUX WebSocket push for %s: %s",
                endpoint_id,
                params,
            )
            self._apply_websocket_params(endpoint_id, params)

    async def async_start_websocket(self) -> None:
        """Start devpush subscription for AUX V2 heat pumps."""
        if self._ws_started:
            return

        await self.api.initialize_websocket()
        self.api.ws_api.add_websocket_listener(self._handle_websocket_message)

        dev_list = []

        for device in self.devices:
            if device.get("productId") != "000000000000000000000000c3aa0000":
                continue

            try:
                extern = json.loads(device.get("extern", "{}"))
            except (TypeError, json.JSONDecodeError):
                extern = {}

            if extern.get("ver") != 2:
                continue

            endpoint_id = device.get("endpointId")
            dev_session = device.get("devSession")

            if not endpoint_id or not dev_session:
                continue

            dev_list.append(
                {
                    "devSession": dev_session,
                    "endpointId": endpoint_id,
                    "gatewayId": "",
                    "pid": device.get("productId"),
                }
            )

        if not dev_list:
            _LOGGER.info("No AUX V2 heat pumps found for WebSocket subscription.")
            return

        await self.api.ws_api.send_data(
            {
                "data": {"devList": dev_list},
                "messageid": str(time.time()),
                "msgtype": "sub",
                "topic": "devpush",
            }
        )

        self._ws_started = True

        _LOGGER.info(
            "Subscribed to AUX devpush for %s V2 heat pump(s).",
            len(dev_list),
        )

    async def _async_update_data(self):
        """Fetch data from AUX Cloud."""
        _LOGGER.debug("Updating AUX Cloud data...")

        try:
            if not self.api.is_logged_in():
                # Attempt to log in
                _LOGGER.debug("Logging into AUX Cloud API...")
                login_success = await self.api.login(self.email, self.password)
                if not login_success:
                    raise UpdateFailed("Login to AUX Cloud API failed")

            if self.api.families is None:
                _LOGGER.debug("Fetching families from AUX Cloud API...")
                await self.api.get_families()

            # Create a single list of tasks for fetching devices (shared and non-shared)
            device_tasks = []

            for family_id in self.api.families:
                device_tasks.append(
                    self.api.get_devices(
                        family_id,
                        shared=False,
                        selected_devices=self.selected_device_ids,
                    )
                )
                device_tasks.append(
                    self.api.get_devices(
                        family_id,
                        shared=True,
                        selected_devices=self.selected_device_ids,
                    )
                )

            # Run all tasks concurrently
            devices_results = await asyncio.gather(
                *device_tasks, return_exceptions=True
            )

            # Process results and handle exceptions
            all_devices = []

            for result in devices_results:
                if isinstance(result, BaseException):
                    _LOGGER.error("Error fetching devices for a family: %s", result)
                    continue
                for device in result:
                    if isinstance(device, Exception):
                        continue
                    if (
                        device["endpointId"] in self.selected_device_ids
                        or not self.selected_device_ids
                    ):
                        all_devices.append(device)

            self.devices = all_devices

            # HTTP polling cannot read V2 heat-pump KeyValueControl state.
            # Preserve the latest state received from devpush.
            for device in self.devices:
                endpoint_id = device.get("endpointId")
                cached_params = self._ws_params.get(endpoint_id)

                if cached_params:
                    device.setdefault("params", {}).update(cached_params)

            _LOGGER.debug("Fetched AUX Cloud data: %s devices", len(self.devices))

            current_endpoint_ids = {
                device["endpointId"]
                for device in self.devices
                if "endpointId" in device
            }
            stale_helpers = set(self._device_state_helpers) - current_endpoint_ids
            for endpoint_id in stale_helpers:
                self._device_state_helpers.pop(endpoint_id, None)

            self.async_set_updated_data({"devices": self.devices})

            return {"devices": self.devices}

        except Exception as e:
            raise UpdateFailed(f"Error updating AUX Cloud data: {e}") from e


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up AUX Cloud from a config entry."""
    region = entry.data.get(CONF_REGION, "eu")
    api = AuxCloudAPI(region=region)
    email = entry.data.get(CONF_EMAIL)
    password = entry.data.get(CONF_PASSWORD)
    selected_device_ids = entry.data.get(CONF_SELECTED_DEVICES, [])

    if not email or not password:
        _LOGGER.error("Missing required credentials for AUX Cloud")
        return False

    coordinator = AuxCloudCoordinator(hass, api, email, password, selected_device_ids)

    # Attempt to log in
    try:
        login_success = await api.login(email, password)
        if not login_success:
            _LOGGER.error("Login to AUX Cloud API failed")
            return False
    except Exception as e:
        _LOGGER.error("Exception during login: %s", e)
        return False

    # Perform an initial update
    await coordinator.async_config_entry_first_refresh()

    # V2 AUX heat pumps publish their usable state through devpush.
    try:
        await coordinator.async_start_websocket()
    except Exception as exc:
        _LOGGER.error("Unable to start AUX Cloud WebSocket: %s", exc)

    # Store the coordinator for platform use
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
        "coordinator": coordinator,
        "api": api,
    }

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload the config entry and platforms."""
    entry_data = hass.data.get(DOMAIN, {}).get(entry.entry_id, {})
    api = entry_data.get("api")

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    if unload_ok:
        if api and api.ws_api:
            await api.ws_api.close_websocket()

        hass.data.get(DOMAIN, {}).pop(entry.entry_id, None)

        if not hass.data.get(DOMAIN):
            hass.data.pop(DOMAIN, None)

    return unload_ok
