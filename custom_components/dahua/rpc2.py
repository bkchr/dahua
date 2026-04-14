"""
Dahua RPC2 API Client

Auth taken and modified and added to, from https://gist.github.com/gxfxyz/48072a72be3a169bc43549e676713201
"""
import hashlib
import json
import logging
import sys
from hashlib import md5
from urllib.parse import quote

import aiohttp

_LOGGER: logging.Logger = logging.getLogger(__package__)

if sys.version_info > (3, 0):
    unicode = str


class DahuaRpc2Client:
    def __init__(
            self,
            username: str,
            password: str,
            address: str,
            port: int,
            rtsp_port: int,
            session: aiohttp.ClientSession
    ) -> None:
        self._username = username
        self._password = password
        self._address = address.rstrip('/')
        self._session = session
        self._port = port
        self._rtsp_port = rtsp_port
        self._session_id = None
        self._id = 0
        protocol = "https" if int(port) == 443 else "http"
        self._base = "{0}://{1}:{2}".format(protocol, self._address, port)

    # =========================================================================
    # Core RPC2 methods (unchanged)
    # =========================================================================

    async def request(self, method, params=None, object_id=None, extra=None, url=None, verify_result=True):
        """Make an RPC request."""
        self._id += 1
        data = {'method': method, 'id': self._id}
        if params is not None:
            data['params'] = params
        if object_id:
            data['object'] = object_id
        if extra is not None:
            data.update(extra)
        if self._session_id:
            data['session'] = self._session_id
        if not url:
            url = "{0}/RPC2".format(self._base)

        resp = await self._session.post(url, data=json.dumps(data))
        resp_json = json.loads(await resp.text())

        if verify_result and resp_json['result'] is False:
            raise ConnectionError(str(resp))

        return resp_json

    async def login(self):
        """Dahua RPC login.
        Reversed from rpcCore.js (login, getAuth & getAuthByType functions).
        Also referenced:
        https://gist.github.com/avelardi/1338d9d7be0344ab7f4280618930cd0d
        """

        # login1: get session, realm & random for real login
        self._session_id = None
        self._id = 0
        url = '{0}/RPC2_Login'.format(self._base)
        method = "global.login"
        params = {'userName': self._username,
                  'password': "",
                  'clientType': "Dahua3.0-Web3.0"}
        r = await self.request(method=method, params=params, url=url, verify_result=False)

        self._session_id = r['session']
        realm = r['params']['realm']
        random = r['params']['random']

        # Password encryption algorithm. Reversed from rpcCore.getAuthByType
        pwd_phrase = self._username + ":" + realm + ":" + self._password
        if isinstance(pwd_phrase, unicode):
            pwd_phrase = pwd_phrase.encode('utf-8')
        pwd_hash = hashlib.md5(pwd_phrase).hexdigest().upper()
        pass_phrase = self._username + ':' + random + ':' + pwd_hash
        if isinstance(pass_phrase, unicode):
            pass_phrase = pass_phrase.encode('utf-8')
        pass_hash = hashlib.md5(pass_phrase).hexdigest().upper()

        # login2: the real login
        params = {'userName': self._username,
                  'password': pass_hash,
                  'clientType': "Dahua3.0-Web3.0",
                  'authorityType': "Default",
                  'passwordType': "Default"}
        return await self.request(method=method, params=params, url=url)

    async def logout(self) -> bool:
        """Logs out of the current session. Returns true if the logout was successful"""
        try:
            response = await self.request(method="global.logout")
            if response['result'] is True:
                return True
            else:
                _LOGGER.debug("Failed to log out of Dahua device %s", self._base)
                return False
        except Exception as exception:
            return False

    # =========================================================================
    # RPC2-specific helpers (used by fallback block in __init__.py)
    # =========================================================================

    async def current_time(self):
        """Get the current time on the device."""
        response = await self.request(method="global.getCurrentTime")
        return response['params']['time']

    async def get_serial_number(self) -> str:
        """Gets the serial number of the device."""
        response = await self.request(method="magicBox.getSerialNo")
        return response['params']['sn']

    async def get_config(self, params):
        """Gets config for the supplied params (raw RPC2 access)."""
        response = await self.request(method="configManager.getConfig", params=params)
        return response['params']

    async def get_device_name(self) -> str:
        """Get the device name (RPC2-specific, returns str)."""
        data = await self.get_config({"name": "General"})
        return data["table"]["MachineName"]

    # =========================================================================
    # Flatten helpers: convert RPC2 JSON to CGI-style flat dict
    # =========================================================================

    def _flatten_config_response(self, config_name, response_params):
        """
        Convert RPC2 JSON config response to flat dict matching CGI format.

        RPC2 returns: {"table": [{"Enable": true}]}
        CGI returns:  {"table.MotionDetect[0].Enable": "true"}

        This method bridges the gap by recursively flattening the nested
        JSON structure into dot-notation keys with array indices.
        """
        result = {}
        table = response_params.get("table")
        self._flatten_value(result, "table.{0}".format(config_name), table)
        return result

    def _flatten_value(self, result, prefix, value):
        """Recursively flatten a nested value into dot-notation keys."""
        if isinstance(value, dict):
            for k, v in value.items():
                self._flatten_value(result, "{0}.{1}".format(prefix, k), v)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                self._flatten_value(result, "{0}[{1}]".format(prefix, i), item)
        else:
            if isinstance(value, bool):
                result[prefix] = str(value).lower()
            elif value is None:
                result[prefix] = ""
            else:
                result[prefix] = str(value)

    # =========================================================================
    # RPC2 setConfig helper
    # =========================================================================

    async def _set_config(self, name, table):
        """Set config via RPC2 configManager.setConfig."""
        try:
            await self.request(
                method="configManager.setConfig",
                params={"name": name, "table": table}
            )
            return {"OK": ""}
        except ConnectionError as e:
            raise aiohttp.ClientError(str(e)) from e

    # =========================================================================
    # CGI-compatible public interface
    # =========================================================================

    def get_rtsp_stream_url(self, channel: int, subtype: int) -> str:
        """Returns the RTSP url for the supplied subtype."""
        url = "rtsp://{0}:{1}@{2}:{3}/cam/realmonitor?channel={4}&subtype={5}".format(
            quote(self._username, safe=''),
            quote(self._password, safe=''),
            self._address,
            self._rtsp_port,
            channel,
            subtype,
        )
        if subtype == 3:
            url = "rtsp://{0}:{1}@{2}".format(
                self._username,
                self._password,
                self._address,
            )
        return url

    async def async_get_snapshot(self, channel_number: int) -> bytes:
        """Snapshots not supported via RPC2."""
        raise aiohttp.ClientError("Snapshots not supported via RPC2")

    async def async_get_system_info(self) -> dict:
        """Get system info. Uses magicBox RPC2 calls."""
        try:
            sn = await self.get_serial_number()
            result = {"serialNumber": sn}
            try:
                dt = await self.request(method="magicBox.getDeviceType")
                result["deviceType"] = dt['params']['type']
            except Exception:
                pass
            return result
        except Exception:
            not_hashed_id = "{0}_{1}_{2}_{3}".format(self._address, self._rtsp_port, self._username, self._password)
            unique_cam_id = md5(not_hashed_id.encode('UTF-8')).hexdigest()
            return {"serialNumber": unique_cam_id}

    async def get_device_type(self) -> dict:
        """Get device type. Returns dict matching CGI format."""
        try:
            response = await self.request(method="magicBox.getDeviceType")
            return {"type": response['params']['type']}
        except Exception:
            return {"type": "Generic RTSP"}

    async def get_software_version(self) -> dict:
        """Get software/firmware version."""
        try:
            response = await self.request(method="magicBox.getSoftwareVersion")
            version = response.get("params", {}).get("version", {})
            if isinstance(version, dict):
                # Some devices return {"Version": "x.y.z", "BuildDate": "..."}
                version = version.get("Version", "1.0")
            return {"version": str(version)}
        except Exception:
            return {"version": "1.0"}

    async def get_machine_name(self) -> dict:
        """Get machine name. Returns dict matching CGI format."""
        try:
            name = await self.get_device_name()
            return {"name": name}
        except Exception:
            not_hashed_id = "{0}_{1}_{2}_{3}".format(self._address, self._rtsp_port, self._username, self._password)
            unique_cam_id = md5(not_hashed_id.encode('UTF-8')).hexdigest()
            return {"name": unique_cam_id}

    async def get_vendor(self) -> dict:
        """Get vendor name."""
        try:
            response = await self.request(method="magicBox.getVendor")
            return {"vendor": response.get("params", {}).get("vendor", "Dahua")}
        except Exception:
            return {"vendor": "Dahua"}

    async def reboot(self) -> dict:
        """Reboot the device."""
        try:
            await self.request(method="magicBox.reboot")
            return {"OK": ""}
        except ConnectionError as e:
            raise aiohttp.ClientError(str(e)) from e

    async def get_max_extra_streams(self) -> int:
        """Get max extra streams. VTO devices typically support fewer streams."""
        return 2

    async def async_get_coaxial_control_io_status(self) -> dict:
        """Get coaxial control IO status. Returns flat dict matching CGI format."""
        try:
            response = await self.request(method="CoaxialControlIO.getStatus", params={"channel": 1})
            status = response.get("params", {}).get("status", {})
            result = {}
            for key, value in status.items():
                result["status.status.{0}".format(key)] = str(value)
            return result
        except ConnectionError as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_get_lighting_v2(self) -> dict:
        """Get Lighting_V2 config."""
        return await self.async_get_config("Lighting_V2")

    async def async_get_machine_name(self) -> dict:
        """Get machine name via configManager. Returns flat dict matching CGI format."""
        try:
            name = await self.get_device_name()
            return {"table.General.MachineName": name}
        except Exception:
            not_hashed_id = "{0}_{1}_{2}_{3}".format(self._address, self._rtsp_port, self._username, self._password)
            unique_cam_id = md5(not_hashed_id.encode('UTF-8')).hexdigest()
            return {"table.General.MachineName": unique_cam_id}

    async def async_get_config(self, name) -> dict:
        """Get config by name via RPC2. Returns flat dict matching CGI format."""
        try:
            response = await self.request(method="configManager.getConfig", params={"name": name})
            return self._flatten_config_response(name, response["params"])
        except ConnectionError as e:
            raise aiohttp.ClientError(str(e)) from e
        except Exception:
            return {}

    async def async_get_config_lighting(self, channel: int, profile_mode) -> dict:
        """Get lighting config for channel and profile mode."""
        try:
            return await self.async_get_config("Lighting[{0}][{1}]".format(channel, profile_mode))
        except aiohttp.ClientError:
            return {}

    async def async_get_config_motion_detection(self) -> dict:
        """Get motion detection config."""
        try:
            return await self.async_get_config("MotionDetect")
        except Exception:
            return {"table.MotionDetect[0].Enable": "false"}

    async def async_get_video_analyse_rules_for_amcrest(self):
        """Get VideoAnalyseRule for Amcrest devices."""
        try:
            return await self.async_get_config("VideoAnalyseRule[0][0].Enable")
        except Exception:
            return {"table.VideoAnalyseRule[0][0].Enable": "false"}

    async def async_get_ivs_rules(self):
        """Get IVS (VideoAnalyseRule) rules."""
        return await self.async_get_config("VideoAnalyseRule")

    async def async_set_all_ivs_rules(self, channel: int, enabled: bool):
        """Set all IVS rules to enabled or disabled."""
        rules = await self.async_get_ivs_rules()
        for index in range(10):
            rule = "table.VideoAnalyseRule[{0}][{1}].Enable".format(channel, index)
            if rule in rules:
                await self.async_set_ivs_rule(channel, index, enabled)

    async def async_set_ivs_rule(self, channel: int, index: int, enabled: bool):
        """Set a single IVS rule."""
        try:
            # Get current config, modify, and set back
            config = await self.get_config({"name": "VideoAnalyseRule"})
            table = config.get("table", [])
            if len(table) > channel and len(table[channel]) > index:
                table[channel][index]["Enable"] = enabled
                await self._set_config("VideoAnalyseRule", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_enabled_smart_motion_detection(self, enabled: bool):
        """Enable or disable smart motion detection."""
        try:
            config = await self.get_config({"name": "SmartMotionDetect"})
            table = config.get("table", [{}])
            if isinstance(table, list) and len(table) > 0:
                table[0]["Enable"] = enabled
            await self._set_config("SmartMotionDetect", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_light_global_enabled(self, enabled: bool):
        """Turn the blue ring light on/off for Amcrest doorbells."""
        try:
            config = await self.get_config({"name": "LightGlobal"})
            table = config.get("table", [{}])
            if isinstance(table, list) and len(table) > 0:
                table[0]["Enable"] = enabled
            await self._set_config("LightGlobal", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_get_smart_motion_detection(self) -> dict:
        """Get smart motion detection config."""
        return await self.async_get_config("SmartMotionDetect")

    async def async_get_ptz_position(self) -> dict:
        """Get PTZ position status."""
        try:
            response = await self.request(method="ptz.getStatus")
            status = response.get("params", {})
            result = {}
            # Flatten the status response to match CGI format: status.X=Y
            for key, value in status.items():
                if isinstance(value, list):
                    for i, item in enumerate(value):
                        result["status.Postion[{0}]".format(i)] = str(item)
                else:
                    result["status.{0}".format(key)] = str(value)
            return result
        except ConnectionError as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_get_light_global_enabled(self) -> dict:
        """Get state of Amcrest blue ring light."""
        return await self.async_get_config("LightGlobal[0].Enable")

    async def async_get_floodlightmode(self) -> dict:
        """Get floodlight mode."""
        try:
            return await self.async_get_config("FloodLightMode.Mode")
        except Exception:
            return {}

    async def async_set_floodlightmode(self, mode: int) -> dict:
        """Set floodlight mode. 1=Motion, 2=Manual, 3=Schedule, 4=PIR."""
        try:
            config = await self.get_config({"name": "FloodLightMode"})
            table = config.get("table", {})
            if isinstance(table, dict):
                table["Mode"] = mode
            await self._set_config("FloodLightMode", table)
            return {"OK": ""}
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_lighting_v1(self, channel: int, enabled: bool, brightness: int) -> dict:
        """Set IR light on or off."""
        mode = "Manual" if enabled else "Off"
        return await self.async_set_lighting_v1_mode(channel, mode, brightness)

    async def async_set_lighting_v1_mode(self, channel: int, mode: str, brightness: int) -> dict:
        """Set IR light mode and brightness."""
        if mode.lower() == "on":
            mode = "Manual"
        mode = mode.capitalize()
        try:
            config = await self.get_config({"name": "Lighting"})
            table = config.get("table", [[{}]])
            # Ensure structure exists for channel
            while len(table) <= channel:
                table.append([{}])
            if isinstance(table[channel], list) and len(table[channel]) > 0:
                table[channel][0]["Mode"] = mode
                if "MiddleLight" not in table[channel][0]:
                    table[channel][0]["MiddleLight"] = [{}]
                table[channel][0]["MiddleLight"][0]["Light"] = brightness
            await self._set_config("Lighting", table)
            return {"OK": ""}
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_goto_preset_position(self, channel: int, position: int) -> dict:
        """Go to a PTZ preset position."""
        try:
            await self.request(
                method="ptz.start",
                params={"channel": channel, "code": "GotoPreset", "arg1": 0, "arg2": position, "arg3": 0}
            )
            return {"OK": ""}
        except ConnectionError as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_video_profile_mode(self, channel: int, mode: str):
        """Set camera profile mode to day or night."""
        mode_val = "1" if mode.lower() == "night" else "0"
        try:
            config = await self.get_config({"name": "VideoInMode"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            if "Config" not in table[channel]:
                table[channel]["Config"] = ["0"]
            table[channel]["Config"][0] = mode_val
            await self._set_config("VideoInMode", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_adjustfocus_v1(self, focus: str, zoom: str):
        """Adjust focus and zoom."""
        try:
            await self.request(
                method="devVideoInput.adjustFocus",
                params={"focus": focus, "zoom": zoom}
            )
        except ConnectionError as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_setprivacymask(self, index: int, enabled: bool):
        """Enable or disable privacy mask."""
        try:
            config = await self.get_config({"name": "PrivacyMasking"})
            table = config.get("table", [[{}]])
            if isinstance(table, list) and len(table) > 0:
                while len(table[0]) <= index:
                    table[0].append({})
                table[0][index]["Enable"] = enabled
            await self._set_config("PrivacyMasking", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_night_switch_mode(self, channel: int, mode: str):
        """Set night switch mode (for Lorex NVR)."""
        mode_val = "3" if mode.lower() == "night" else "0"
        try:
            config = await self.get_config({"name": "VideoInOptions"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            if "NightOptions" not in table[channel]:
                table[channel]["NightOptions"] = {}
            table[channel]["NightOptions"]["SwitchMode"] = int(mode_val)
            await self._set_config("VideoInOptions", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_enable_channel_title(self, channel: int, enabled: bool):
        """Enable or disable channel title overlay."""
        try:
            config = await self.get_config({"name": "VideoWidget"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            if "ChannelTitle" not in table[channel]:
                table[channel]["ChannelTitle"] = {}
            table[channel]["ChannelTitle"]["EncodeBlend"] = enabled
            await self._set_config("VideoWidget", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_enable_time_overlay(self, channel: int, enabled: bool):
        """Enable or disable time overlay."""
        try:
            config = await self.get_config({"name": "VideoWidget"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            if "TimeTitle" not in table[channel]:
                table[channel]["TimeTitle"] = {}
            table[channel]["TimeTitle"]["EncodeBlend"] = enabled
            await self._set_config("VideoWidget", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_enable_text_overlay(self, channel: int, group: int, enabled: bool):
        """Enable or disable text overlay."""
        try:
            config = await self.get_config({"name": "VideoWidget"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            if "CustomTitle" not in table[channel]:
                table[channel]["CustomTitle"] = [{}]
            while len(table[channel]["CustomTitle"]) <= group:
                table[channel]["CustomTitle"].append({})
            table[channel]["CustomTitle"][group]["EncodeBlend"] = enabled
            await self._set_config("VideoWidget", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_enable_custom_overlay(self, channel: int, group: int, enabled: bool):
        """Enable or disable custom overlay."""
        try:
            config = await self.get_config({"name": "VideoWidget"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            if "UserDefinedTitle" not in table[channel]:
                table[channel]["UserDefinedTitle"] = [{}]
            while len(table[channel]["UserDefinedTitle"]) <= group:
                table[channel]["UserDefinedTitle"].append({})
            table[channel]["UserDefinedTitle"][group]["EncodeBlend"] = enabled
            await self._set_config("VideoWidget", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_service_set_channel_title(self, channel: int, text1: str, text2: str):
        """Set channel title."""
        text = '|'.join(filter(None, [text1, text2]))
        try:
            config = await self.get_config({"name": "ChannelTitle"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            table[channel]["Name"] = text
            await self._set_config("ChannelTitle", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_service_set_text_overlay(self, channel: int, group: int, text1: str, text2: str, text3: str,
                                                 text4: str):
        """Set video text overlay."""
        text = '|'.join(filter(None, [text1, text2, text3, text4]))
        try:
            config = await self.get_config({"name": "VideoWidget"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            if "CustomTitle" not in table[channel]:
                table[channel]["CustomTitle"] = [{}]
            while len(table[channel]["CustomTitle"]) <= group:
                table[channel]["CustomTitle"].append({})
            table[channel]["CustomTitle"][group]["Text"] = text
            await self._set_config("VideoWidget", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_service_set_custom_overlay(self, channel: int, group: int, text1: str, text2: str):
        """Set custom overlay text."""
        text = '|'.join(filter(None, [text1, text2]))
        try:
            config = await self.get_config({"name": "VideoWidget"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            if "UserDefinedTitle" not in table[channel]:
                table[channel]["UserDefinedTitle"] = [{}]
            while len(table[channel]["UserDefinedTitle"]) <= group:
                table[channel]["UserDefinedTitle"].append({})
            table[channel]["UserDefinedTitle"][group]["Text"] = text
            await self._set_config("VideoWidget", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_lighting_v2(self, channel: int, enabled: bool, brightness: int, profile_mode: str) -> dict:
        """Set white light on/off with brightness."""
        mode = "Manual" if enabled else "Off"
        try:
            config = await self.get_config({"name": "Lighting_V2"})
            table = config.get("table", [[[{}]]])
            # Ensure structure: table[channel][profile_mode][0]
            while len(table) <= channel:
                table.append([[{}]])
            while len(table[channel]) <= int(profile_mode):
                table[channel].append([{}])
            if len(table[channel][int(profile_mode)]) == 0:
                table[channel][int(profile_mode)].append({})
            entry = table[channel][int(profile_mode)][0]
            entry["Mode"] = mode
            if "MiddleLight" not in entry:
                entry["MiddleLight"] = [{}]
            entry["MiddleLight"][0]["Light"] = brightness
            await self._set_config("Lighting_V2", table)
            return {"OK": ""}
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_lighting_v2_for_flood_lights(self, channel: int, enabled: bool, profile_mode: str) -> dict:
        """Set flood light on/off."""
        mode = "Manual" if enabled else "Off"
        try:
            config = await self.get_config({"name": "Lighting_V2"})
            table = config.get("table", [[[{}, {}]]])
            while len(table) <= channel:
                table.append([[{}, {}]])
            while len(table[channel]) <= int(profile_mode):
                table[channel].append([{}, {}])
            while len(table[channel][int(profile_mode)]) <= 1:
                table[channel][int(profile_mode)].append({})
            table[channel][int(profile_mode)][1]["Mode"] = mode
            await self._set_config("Lighting_V2", table)
            return {"OK": ""}
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_lighting_v2_for_amcrest_doorbells(self, mode: str) -> dict:
        """Set white light on Amcrest doorbells. mode: On, Off, Flicker."""
        mode = mode.lower()
        cmd_mode = "Off"
        state = None
        if mode == "on":
            cmd_mode = "ForceOn"
            state = "On"
        elif mode in ('strobe', 'flicker'):
            cmd_mode = "ForceOn"
            state = "Flicker"
        try:
            config = await self.get_config({"name": "Lighting_V2"})
            table = config.get("table", [[[{}, {}]]])
            # Access [0][0][1] for doorbell light
            while len(table) == 0:
                table.append([[{}, {}]])
            while len(table[0]) == 0:
                table[0].append([{}, {}])
            while len(table[0][0]) <= 1:
                table[0][0].append({})
            table[0][0][1]["Mode"] = cmd_mode
            if state is not None:
                table[0][0][1]["State"] = state
            await self._set_config("Lighting_V2", table)
            return {"OK": ""}
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_video_in_day_night_mode(self, channel: int, config_type: str, mode: str):
        """Set video day/night mode."""
        if config_type == "day":
            config_no = 0
        elif config_type == "night":
            config_no = 1
        else:
            config_no = 2

        if mode is None or mode.lower() == "auto" or mode.lower() == "brightness":
            mode = "Brightness"
        elif mode.lower() == "color":
            mode = "Color"
        elif mode.lower() == "blackwhite":
            mode = "BlackWhite"

        try:
            config = await self.get_config({"name": "VideoInDayNight"})
            table = config.get("table", [[{}]])
            while len(table) <= channel:
                table.append([{}])
            if isinstance(table[channel], list):
                while len(table[channel]) <= config_no:
                    table[channel].append({})
                table[channel][config_no]["Mode"] = mode
            await self._set_config("VideoInDayNight", table)
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_get_video_in_mode(self) -> dict:
        """Get video in mode (profile mode day/night)."""
        return await self.async_get_config("VideoInMode")

    async def async_set_coaxial_control_state(self, channel: int, dahua_type: int, enabled: bool) -> dict:
        """Set coaxial control state (white light / siren)."""
        io = "1" if enabled else "2"
        try:
            await self.request(
                method="CoaxialControlIO.control",
                params={"channel": channel, "info": [{"Type": dahua_type, "IO": io}]}
            )
            return {"OK": ""}
        except ConnectionError as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_disarming_linkage(self, channel: int, enabled: bool) -> dict:
        """Set disarming linkage."""
        try:
            config = await self.get_config({"name": "DisableLinkage"})
            table = config.get("table", {})
            if isinstance(table, list):
                while len(table) <= channel:
                    table.append({})
                table[channel]["Enable"] = enabled
            elif isinstance(table, dict):
                table["Enable"] = enabled
            await self._set_config("DisableLinkage", table)
            return {"OK": ""}
        except ConnectionError:
            # Retry without channel index
            try:
                await self._set_config("DisableLinkage", {"Enable": enabled})
                return {"OK": ""}
            except Exception as e:
                raise aiohttp.ClientError(str(e)) from e
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_event_notifications(self, channel: int, enabled: bool) -> dict:
        """Set event notifications. Note: enabled=True means DisableEventNotify.Enable=false."""
        value = not enabled
        try:
            config = await self.get_config({"name": "DisableEventNotify"})
            table = config.get("table", {})
            if isinstance(table, list):
                while len(table) <= channel:
                    table.append({})
                table[channel]["Enable"] = value
            elif isinstance(table, dict):
                table["Enable"] = value
            await self._set_config("DisableEventNotify", table)
            return {"OK": ""}
        except ConnectionError:
            try:
                await self._set_config("DisableEventNotify", {"Enable": value})
                return {"OK": ""}
            except Exception as e:
                raise aiohttp.ClientError(str(e)) from e
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_set_record_mode(self, channel: int, mode: str) -> dict:
        """Set record mode: auto, manual/on, or off."""
        if mode.lower() == "auto":
            mode_val = 0
        elif mode.lower() in ("manual", "on"):
            mode_val = 1
        else:
            mode_val = 2
        try:
            config = await self.get_config({"name": "RecordMode"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            table[channel]["Mode"] = mode_val
            await self._set_config("RecordMode", table)
            return {"OK": ""}
        except Exception as e:
            raise aiohttp.ClientError(str(e)) from e

    async def async_get_disarming_linkage(self) -> dict:
        """Get disarming linkage config."""
        return await self.async_get_config("DisableLinkage")

    async def async_get_event_notifications(self) -> dict:
        """Get event notifications config."""
        return await self.async_get_config("DisableEventNotify")

    async def async_access_control_open_door(self, door_id: int = 1) -> dict:
        """Open a door via VTO access control."""
        try:
            await self.request(
                method="accessControl.openDoor",
                params={"UserID": 101, "Type": "Remote", "channel": door_id}
            )
            return {"OK": ""}
        except ConnectionError as e:
            raise aiohttp.ClientError(str(e)) from e

    async def enable_motion_detection(self, channel: int, enabled: bool) -> dict:
        """Enable or disable motion detection."""
        try:
            config = await self.get_config({"name": "MotionDetect"})
            table = config.get("table", [{}])
            while len(table) <= channel:
                table.append({})
            table[channel]["Enable"] = enabled
            table[channel]["DetectVersion"] = "V3.0"
            await self._set_config("MotionDetect", table)
            return {"OK": ""}
        except Exception as e:
            # Retry without DetectVersion for older devices
            try:
                config = await self.get_config({"name": "MotionDetect"})
                table = config.get("table", [{}])
                while len(table) <= channel:
                    table.append({})
                table[channel]["Enable"] = enabled
                await self._set_config("MotionDetect", table)
                return {"OK": ""}
            except Exception as e2:
                raise aiohttp.ClientError(str(e2)) from e2

    async def stream_events(self, on_receive, events: list, channel: int):
        """VTO devices use the binary protocol for events, not CGI event streaming."""
        raise NotImplementedError("VTO devices use the binary VTO protocol for events, not CGI event streaming")

    @staticmethod
    async def parse_dahua_api_response(data: str) -> dict:
        """Parse key=value text response to dict. Included for interface parity."""
        lines = data.splitlines()
        data_dict = {}
        for line in lines:
            parts = line.split("=", 1)
            if len(parts) == 2:
                data_dict[parts[0]] = parts[1]
            else:
                data_dict[parts[0]] = line
        return data_dict

    @staticmethod
    def to_stream_name(subtype: int) -> str:
        """Given the subtype (stream index), returns the stream name."""
        if subtype == 0:
            return "Main"
        elif subtype == 1:
            return "Sub"
        else:
            return "Sub_{0}".format(subtype)
