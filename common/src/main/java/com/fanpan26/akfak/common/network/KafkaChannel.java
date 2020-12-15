package com.fanpan26.akfak.common.network;

import com.fanpan26.akfak.common.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.security.Principal;

/**
 * @author fanyuepan
 */
public class KafkaChannel {

    private final String id;
    private final TransportLayer transportLayer;
    private final Authenticator authenticator;
    private final int maxReceiveSize;
    private NetworkReceive receive;
    private Send send;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
    }

    public void close() throws IOException {
        Utils.closeAll(transportLayer,authenticator);
    }

    public Principal principal() throws IOException{
        return authenticator.principal();
    }

    public void prepare() throws IOException {
        if (!transportLayer.ready()){
            transportLayer.handshake();
        }
        if (transportLayer.ready() && !authenticator.complete()){
            authenticator.authenticate();
        }
    }

    public void disconnect(){
         transportLayer.disconnect();
    }

    public boolean finishConnect() throws IOException{
        return transportLayer.finishConnect();
    }

    public String id(){
        return id;
    }

    public void mute(){
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute(){
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public boolean isMute(){
        return transportLayer.isMute();
    }

    public boolean ready(){
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend(){
        return send != null;
    }

    public InetAddress socketAddress(){
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription(){
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null){
            return socket.getLocalAddress().toString();
        }
        return socket.getInetAddress().toString();
    }

    public void setSend(Send send){
        //上一个消息还没有发送完，不能发送
        if (this.send != null){
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        }
        this.send = send;
        //注册OP_WRITE事件
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    public Send write() throws IOException {
        Send result =  null;
        if (send != null && send(send)){
            result = send;
            send = null;
        }
        return result;
    }

    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    /**
     * 将消息写入，有可能一次写不完，下次循环继续写
     * */
    private boolean send(Send send) throws IOException {
        send.writeTo(transportLayer);
        if (send.completed()) {
            //写完之后，注销 OP_WRITE
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
        }
        return send.completed();
    }

    public boolean isConnected(){
        return transportLayer.isConnected();
    }

    public NetworkReceive read() throws IOException{
        NetworkReceive result = null;
        if (receive == null){
            receive = new NetworkReceive(maxReceiveSize,id);
        }
        //读取信息
        receive(receive);
        //如果已经读完了，可以直接返回一个完整的NetworkReceive，否则等待下一次继续读取数据
        if (receive.complete()){
            receive.payload().rewind();
            result = receive;
            receive = null;
        }
        return result;
    }
}
