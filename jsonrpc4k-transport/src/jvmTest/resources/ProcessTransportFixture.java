import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Child process for ProcessTransportTest, started with the single-file source launcher. */
public class ProcessTransportFixture {
    public static void main(String[] args) throws Exception {
        InputStream in = System.in;
        OutputStream out = new FileOutputStream(FileDescriptor.out);
        switch (args[0]) {
            case "echo" -> echo(in, out);
            case "exit" -> exitAfterTrigger(in, out);
            case "hold" -> hold(in, out, false);
            case "stubborn" -> hold(in, out, true);
            case "flood" -> flood(out);
            case "info" -> info(out);
            default -> throw new IllegalArgumentException("unknown mode " + args[0]);
        }
    }

    /** Copies stdin to stdout until EOF, then exits with 0. */
    private static void echo(InputStream in, OutputStream out) throws Exception {
        byte[] buffer = new byte[8192];
        for (int count = in.read(buffer); count != -1; count = in.read(buffer)) {
            out.write(buffer, 0, count);
            out.flush();
        }
    }

    /** Waits for one byte, writes "bye" and exits with 3 while the parent is reading. */
    private static void exitAfterTrigger(InputStream in, OutputStream out) throws Exception {
        if (in.read() == -1) throw new IllegalStateException("missing trigger");
        write(out, "bye");
        System.exit(3);
    }

    /** Keeps running with stdout open after stdin EOF; optionally survives SIGTERM until SIGKILL. */
    private static void hold(InputStream in, OutputStream out, boolean ignoreTermination) throws Exception {
        if (ignoreTermination) {
            Runtime.getRuntime().addShutdownHook(new Thread(ProcessTransportFixture::sleepForever));
        }
        write(out, "ready\n");
        while (in.read() != -1) {
            // Discard input: stdin EOF does not end this child.
        }
        sleepForever();
    }

    /** Writes 4 MiB of 'e' to stderr before the only stdout output, so an undrained stderr pipe deadlocks. */
    private static void flood(OutputStream out) throws Exception {
        OutputStream err = new FileOutputStream(FileDescriptor.err);
        byte[] chunk = new byte[64 * 1024];
        Arrays.fill(chunk, (byte) 'e');
        for (int i = 0; i < 64; i++) {
            err.write(chunk);
        }
        err.flush();
        write(out, "done\n");
    }

    /** Reports the fixture variable, whether PATH is set, and the working directory. */
    private static void info(OutputStream out) throws Exception {
        String path = System.getenv("PATH") == null ? "absent" : "present";
        write(out, System.getenv("JSONRPC4K_FIXTURE") + "\n" + path + "\n" + System.getProperty("user.dir") + "\n");
    }

    private static void sleepForever() {
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private static void write(OutputStream out, String text) throws Exception {
        out.write(text.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }
}
