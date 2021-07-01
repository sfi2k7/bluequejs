const WebSocket = require("ws");

function WebSocketClient() {
	this.number = 0;	// Message number
	this.autoReconnectInterval = 5 * 1000;	// ms
}

WebSocketClient.prototype.open = function (url) {
	this.url = url;
	this.instance = new WebSocket(this.url);
	this.instance.addEventListener('open', this.onopen.bind(this));
	this.instance.addEventListener('message', this._onmessage.bind(this));
	this.instance.addEventListener('close', this._onclose.bind(this));
	this.instance.addEventListener('error', this._onerror.bind(this));
}

WebSocketClient.prototype._onerror = function (e) {
	switch (e.code) {
		case 'ECONNREFUSED':
			this.reconnect(e);
			break;
		default:
			this.onerror(e);
			break;
	}
}

WebSocketClient.prototype._onclose = function (e) {
	switch (e.code) {
		case 1000:	// CLOSE_NORMAL
			console.log("WebSocket: closed");
			break;
		default:	// Abnormal closure
			this.reconnect(e);
			break;
	}
	this.onclose(e);
}

WebSocketClient.prototype._onmessage = function (data, flags) {
	this.number++;
	let ds = data.data;
	let msg = JSON.parse(ds);
	if (typeof msg.action === "undefined" || msg.action.length == 0) {
		console.log("Unknown Message", msg);
		return;
	}

	switch (msg.action) {
		case "welcome": this.onwelcome(msg); break;
		case "sub_ok": this.onsubok(msg); break;
		case "info": this.oninfo(msg); break;
		case "payload": this.onpayload(msg); break;
		case "incoming": this.onincoming(msg); break;
		case "packet": this.onpacket(msg); break;
		case "assign":
			if (typeof msg.packet === "undefined" || msg.packet === false) {
				this.ontaskassign(msg);
				return;
			}
			this.onpacket(msg);
			break;
		default:
			this.onunknownaction(msg);
	}

	this.onmessage(msg);
}

WebSocketClient.prototype.send = function (data, option) {
	try {
		this.instance.send(data, option);
	} catch (e) {
		this.instance.emit('error', e);
	}
}

WebSocketClient.prototype.close = function () {
	try {
		this.instance.close(1000);
	} catch (e) {
		this.instance.emit('error', e);
	}
}

WebSocketClient.prototype.packet = function (msg) {
	try {
		this.instance.send(JSON.stringify({ ...msg, action: 'route', packet: true }));
	} catch (e) {
		this.instance.emit('error', e);
	}
}

WebSocketClient.prototype.sub = function (channel) {
	try {
		this.instance.send(JSON.stringify({ action: 'sub', channel }));
	} catch (e) {
		this.instance.emit('error', e);
	}
}

WebSocketClient.prototype.ack = function (id) {
	try {
		this.instance.send(JSON.stringify({ action: 'ack', id }));
	} catch (e) {
		this.instance.emit('error', e);
	}
}

WebSocketClient.prototype.info = function (channel) {
	try {
		this.instance.send(JSON.stringify({channel, action: 'info' }));
	} catch (e) {
		this.instance.emit('error', e);
	}
}

WebSocketClient.prototype.route = function (msg) {
	try {
		this.instance.send(JSON.stringify({ ...msg, action: 'route' }));
	} catch (e) {
		this.instance.emit('error', e);
	}
}

WebSocketClient.prototype.get = function (channel,count) {
	try {
		this.instance.send(JSON.stringify({ data: { count },channel, action: 'get' }));
	} catch (e) {
		this.instance.emit('error', e);
	}
}

WebSocketClient.prototype.task = function (msg) {
	try {
		this.instance.send(JSON.stringify({ ...msg, action: 'task' }));
	} catch (e) {
		this.instance.emit('error', e);
	}
}

WebSocketClient.prototype.reconnect = function (e) {
	console.log(`WebSocketClient: retry in ${this.autoReconnectInterval}ms`, e);

	this.instance.removeEventListener("open", this.onopen)
	this.instance.removeEventListener("close", this._onclose)
	this.instance.removeEventListener("error", this._onerror)
	this.instance.removeEventListener("message", this._onmessage)

	var that = this;
	setTimeout(function () {
		console.log("WebSocketClient: reconnecting...");
		that.open(that.url);
	}, this.autoReconnectInterval);
}

WebSocketClient.prototype.onopen = function (e) { }
WebSocketClient.prototype.onmessage = function (data, flags, number) { }
WebSocketClient.prototype.onerror = function (e) { }
WebSocketClient.prototype.onclose = function (e) { }

WebSocketClient.prototype.onwelcome = function (e) { console.log("welcome", arguments); }
WebSocketClient.prototype.oninfo = function (e) { console.log("info", arguments); }
WebSocketClient.prototype.onincoming = function (e) { console.log("incoming", arguments); }
WebSocketClient.prototype.onpacket = function (e) { console.log("packet", arguments); }
WebSocketClient.prototype.onpayload = function (e) { console.log("payload", arguments); }
WebSocketClient.prototype.onsubok = function (e) { console.log("subok", arguments); }
WebSocketClient.prototype.ontaskassign = function (e) { console.log("assign", arguments); }
WebSocketClient.prototype.onpacket = function (e) { console.log("packet", arguments); }
WebSocketClient.prototype.onunknownaction = function (e) { console.log("unknown action", e); }

const WS = new WebSocketClient();

class Batch {
    constructor() {
        this.list = [];
    }

    assign(items) {
        this.list.push(...items);
    }


    next() {
        if (this.list.length == 0) {
            return null;
        }
        return this.list.splice(0, 1);
    }

    len() {
        return this.list.length;
    }
}

const batch = new Batch();

const data = {
    channelToSubscribe: null,
    isSubscribed: false,
    isConnected: false,
    wsid: null,
    hasItems: false,
    lastItemCount: 0,
    currentBatch: batch,
    isPaused: false,
    piMS: 1000
}

const setData = (key, value) => {
    data[key] = value;
}

const getData = (key) => {
    return data[key]
}

const dumpData = () => {
    console.log(data);
}

const DataKeys = {
    channelToSubscribe: "channelToSubscribe",
    IsSubscribed: "isSubscribed",
    IsConnected: "isConnected",
    WebsocketId: "wsid",
    HasItemsOnServer: "hasItems",
    ItemCountOnServer: "lastItemCount",
    CurrentBatch: "currentBatch",
    IsPaused: "isPaused",
    ProcessIntervalMS: "piMS"
}

const onWelcome = (msg) => {
    setData(DataKeys.WebsocketId, msg.id);
    setData(DataKeys.IsConnected, true);
    WS.sub(getData(DataKeys.channelToSubscribe));
}

const onSubOk = (msg) => {
    setData(DataKeys.IsSubscribed, true);
}

const onInfo = (msg) => {
    setData(DataKeys.ItemCountOnServer, msg.data.count);
    setData(DataKeys.HasItemsOnServer, getData(DataKeys.ItemCountOnServer) > 0);
}

const onIncoming = (msg) => {
    WS.info(getData(DataKeys.channelToSubscribe));
}

const onPayload = (msg) => {
    if (msg.data.count == 0){
        return;
    }


    getData(DataKeys.CurrentBatch).assign(msg.data.list);
}

class Que {
    constructor(channel) {
        this.channel = channel;
        this.jobHandler = null;
        this.processInterval = null;
        this._isRunning = false;
    }

    setUnknownActionHandler(h) {
        this.unknownactionhandler = h;
    }

    setMessageHandler(h) {
        this.messagehandler = h
    }

    onMessagehandler(){

    }

    onPacketHandler(){

    }

    onUnknwonActionHandler(){

    }

    setJobHandler(h) {
        this.jobHandler = h;
    }

    setChannel(channel) {
        this.channel = channel;
    }

    async _localHandler() {
        if (this._isRunning == true || getData(DataKeys.IsPaused) == true) {
            return;
        }

        let isSubscribed = getData(DataKeys.IsSubscribed);
        let isConnected = getData(DataKeys.IsConnected);
        let serverHasJobs = getData(DataKeys.HasItemsOnServer);
        let jobsOnServer = getData(DataKeys.ItemCountOnServer);
        let localJobCount = getData(DataKeys.CurrentBatch).len();

        if (isSubscribed == false || isConnected == false) {
            return;
        }

        if (serverHasJobs == false && jobsOnServer == 0 && localJobCount == 0) {
            return;
        }

        if (localJobCount == 0) {
            WS.get(getData(DataKeys.channelToSubscribe), 10);
            return;
        }

        this._isRunning = true;

        while (true) {
            let currentJob = getData(DataKeys.CurrentBatch).next();
            if (currentJob == null) {
                return;
            }
            await this.jobHandler(currentJob);
        }
    }

    async start(url) {
        setData(DataKeys.channelToSubscribe, this.channel);

        WS.onwelcome = onWelcome;
        WS.onsubok = onSubOk;
        WS.oninfo = onInfo;
        WS.onincoming = onIncoming;
        WS.onpayload = onPayload;

        WS.onunknownaction = this.onUnknwonActionHandler;
        WS.onmessage = this.onMessagehandler;
        WS.onpacket = this.onPacketHandler;

        WS.onclose = () => {
            setData(DataKeys.IsConnected, false)
        }

        WS.open(url)

        this.processInterval = setInterval(async () => await this._localHandler(), getData(DataKeys.ProcessIntervalMS));
    }

    closeOnInt() {
        process.on('SIGTERM', this._shutDown);
        process.on('SIGINT', this._shutDown);
    }

    _shutDown() {
        setData(DataKeys.IsPaused, true);
        clearInterval(processInternal);
        WS.close();
        process.exit(0);
    }
}

module.exports = Que