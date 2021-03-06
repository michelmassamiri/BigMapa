package bigdata;

import java.io.Serializable;

public class Tile implements Serializable {
    private int x;
    private int y;
    private int zoom;
    private byte[] image;

    public Tile(int x, int y, int zoom, byte[] image) {
        this.x = x;
        this.y = y;
        this.zoom = zoom;
        this.image = image;
    }

    public Tile(int x, int y, int zoom) {
        this.x = x;
        this.y = y;
        this.zoom = zoom;
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public int getZoom() {
        return zoom;
    }

    public byte[] getImage() {
        return image;
    }

    public void setX(int x) {
        this.x = x;
    }

    public void setY(int y) {
        this.y = y;
    }

    public void setZoom(int zoom) {
        this.zoom = zoom;
    }

    public void setImage(byte[] image) {
        this.image = image;
    }
}
