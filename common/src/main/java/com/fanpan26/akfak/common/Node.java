package com.fanpan26.akfak.common;

/**
 * @author fanyuepan
 */
public class Node {

    private static final Node NO_NODE = new Node(-1, "", -1);

    private final int id;
    private final String idString;
    private final String host;
    private final int port;

    /**
     * 机架
     * */
    private final String rack;

    public Node(int id, String host, int port) {
        this(id, host, port, null);
    }

    public Node(int id, String host, int port, String rack) {
        super();
        this.id = id;
        this.idString = Integer.toString(id);
        this.host = host;
        this.port = port;
        this.rack = rack;
    }

    public static Node noNode() {
        return NO_NODE;
    }

    public boolean isEmpty() {
        return host == null || host.isEmpty() || port < 0;
    }

    public int id() {
        return id;
    }

    public String idString() {
        return idString;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public boolean hasRack() {
        return rack != null;
    }

    public String rack() {
        return rack;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + id;
        result = prime * result + port;
        result = prime * result + ((rack == null) ? 0 : rack.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Node other = (Node) obj;
        if (host == null) {
            if (other.host != null) {
                return false;
            }
        } else if (!host.equals(other.host)) {
            return false;
        }
        if (id != other.id) {
            return false;
        }
        if (port != other.port) {
            return false;
        }
        if (rack == null) {
            if (other.rack != null) {
                return false;
            }
        } else if (!rack.equals(other.rack)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return host + ":" + port + " (id: " + idString + " rack: " + rack + ")";
    }
}
