package io.kapture.tap;

import com.sun.tools.attach.VirtualMachine;

import java.io.File;
import java.net.URISyntaxException;

/**
 * Dynamic-attach entrypoint for the Kapture JVM tap agent.
 *
 * Invoked by Kapture (Rust side) as:
 *
 *   java -jar kapture-jvm-agent.jar attach &lt;target-pid&gt; &lt;agent-args&gt;
 *
 * Loads this same JAR into the target JVM as an agent. Inside the
 * target, the JDK calls our {@link Agent#agentmain(String, java.lang.instrument.Instrumentation)}
 * (identical to {@code premain}), which installs the ByteBuddy hooks
 * on {@code SslTransportLayer} / {@code PlaintextTransportLayer}.
 *
 * Prerequisites in the target JVM:
 *   * JDK (not JRE-only) — {@code jdk.attach} module needs the
 *     attach-mechanism native bits.
 *   * {@code -XX:-DisableAttachMechanism} (default on; some prod
 *     hardenings disable it).
 *   * Same UID as Kapture, or root.
 *
 * Exit codes:
 *   0  attach succeeded
 *   2  usage error
 *   3  attach failed (target refuses, wrong pid, JRE-only, etc.)
 */
public final class Attacher {

    private Attacher() {}

    public static void main(String[] args) {
        if (args.length < 2 || !"attach".equals(args[0])) {
            System.err.println(
                "usage: java -jar kapture-jvm-agent.jar attach <pid> [<agent-args>]");
            System.exit(2);
        }
        String pid = args[1];
        String agentArgs = args.length >= 3 ? args[2] : "";

        String selfPath;
        try {
            selfPath = new File(
                Attacher.class.getProtectionDomain().getCodeSource().getLocation().toURI()
            ).getAbsolutePath();
        } catch (URISyntaxException e) {
            System.err.println("[attacher] cannot resolve self-jar path: " + e.getMessage());
            System.exit(3);
            return;
        }

        // Always include the JAR path itself as an agent arg so the
        // loaded Agent can build a `ClassFileLocator.ForJarFile` even
        // when its own ProtectionDomain.getCodeSource() returns null
        // (which happens when the JAR sits on the boot classpath or
        // when the dynamic-attach loader strips the code-source link).
        String fullAgentArgs = agentArgs.isEmpty()
            ? "kapture.tap.agentJar=" + selfPath
            : "kapture.tap.agentJar=" + selfPath + "," + agentArgs;

        VirtualMachine vm = null;
        try {
            vm = VirtualMachine.attach(pid);
            vm.loadAgent(selfPath, fullAgentArgs);
            System.out.println("[attacher] OK pid=" + pid + " jar=" + selfPath);
        } catch (Throwable t) {
            // Pretty-print the cause chain so the Rust side can show
            // a useful message in the UI without parsing stack traces.
            System.err.println("[attacher] FAILED pid=" + pid + ": " + t.getClass().getSimpleName()
                + ": " + t.getMessage());
            Throwable cause = t.getCause();
            while (cause != null && cause != t) {
                System.err.println("[attacher]   caused by: " + cause.getClass().getSimpleName()
                    + ": " + cause.getMessage());
                cause = cause.getCause();
            }
            System.exit(3);
        } finally {
            if (vm != null) {
                try { vm.detach(); } catch (Throwable ignored) {}
            }
        }
    }
}
