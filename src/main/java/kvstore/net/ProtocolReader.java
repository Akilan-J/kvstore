package kvstore.net;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Reads the framing used by the wire protocol: CRLF-terminated command lines,
 * plus length-prefixed binary payloads.
 *
 * <p>Values are length-prefixed rather than delimited so that a value may contain
 * arbitrary bytes — including CR, LF, and spaces — without any escaping.
 */
public final class ProtocolReader {

    /** Bounds the line buffer so a malicious client can't force unbounded growth. */
    private static final int MAX_LINE_BYTES = 8 * 1024;

    private final InputStream in;

    public ProtocolReader(InputStream in) {
        this.in = in;
    }

    /**
     * Reads one line, stripping a trailing CRLF or LF.
     *
     * @return the line, or null on a clean end of stream
     */
    public String readLine() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(64);
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                byte[] bytes = buf.toByteArray();
                int len = bytes.length;
                if (len > 0 && bytes[len - 1] == '\r') {
                    len--; // tolerate bare LF as well as CRLF
                }
                return new String(bytes, 0, len, StandardCharsets.UTF_8);
            }
            buf.write(b);
            if (buf.size() > MAX_LINE_BYTES) {
                throw new IOException("command line exceeds " + MAX_LINE_BYTES + " bytes");
            }
        }
        if (buf.size() == 0) {
            return null; // peer closed at a frame boundary
        }
        throw new EOFException("stream ended mid-line");
    }

    /** Reads exactly {@code n} bytes or throws. */
    public byte[] readExactly(int n) throws IOException {
        byte[] buf = new byte[n];
        int off = 0;
        while (off < n) {
            int read = in.read(buf, off, n - off);
            if (read < 0) {
                throw new EOFException("stream ended after " + off + " of " + n + " payload bytes");
            }
            off += read;
        }
        return buf;
    }

    /** Consumes the CRLF that terminates a payload, tolerating a bare LF. */
    public void consumeTerminator() throws IOException {
        int b = in.read();
        if (b == '\r') {
            b = in.read();
        }
        if (b != '\n') {
            throw new IOException("payload not terminated by CRLF");
        }
    }
}
