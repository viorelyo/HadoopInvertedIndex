package model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LineWrapper implements Writable {

    private String lineString;
    private long offset;

    public LineWrapper() {
    }

    public LineWrapper(String lineString, long offset) {
        this.lineString = lineString;
        this.offset = offset;
    }

    public LineWrapper(LineWrapper other) {
        this.lineString = other.getLineString();
        this.offset = other.getOffset();
    }

    public String getLineString() {
        return lineString;
    }

    public void setLineString(String lineString) {
        this.lineString = lineString;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.lineString);
        dataOutput.writeLong(this.offset);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.lineString = dataInput.readUTF();
        this.offset = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "OffsetRecord{" +
                "lineString='" + lineString + '\'' +
                ", offset=" + offset +
                '}';
    }
}