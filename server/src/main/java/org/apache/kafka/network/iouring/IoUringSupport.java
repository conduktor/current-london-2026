/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.network.iouring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Locale;

/**
 * Runtime probe for whether the Netty io_uring transport is usable on this JVM/host.
 *
 * <p>The probe is reflective so this class itself loads on any platform — including
 * macOS / Windows / Linux JVMs that don't ship the io_uring native library — without
 * triggering a NoClassDefFoundError.
 *
 * <p>The result is cached; the underlying conditions (OS, kernel, native lib) don't
 * change during a JVM run.
 */
public final class IoUringSupport {

    private static final Logger LOG = LoggerFactory.getLogger(IoUringSupport.class);
    private static final String IO_URING_PROBE_CLASS = "io.netty.channel.uring.IoUring";

    private static final boolean LINUX =
        System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux");

    private static final Probe PROBE = computeProbe();

    private IoUringSupport() { }

    /** @return true iff this JVM is running on Linux. */
    public static boolean isLinux() {
        return LINUX;
    }

    /**
     * @return true iff the Netty io_uring transport reports itself usable on this host
     *         (Linux kernel with io_uring support and the native lib loadable).
     */
    public static boolean isAvailable() {
        return PROBE.available;
    }

    /**
     * @return a human-readable reason why io_uring is unavailable, or {@code null} when
     *         {@link #isAvailable()} is {@code true}. The reason is suitable for inclusion
     *         in startup logs and error messages.
     */
    public static String unavailabilityReason() {
        return PROBE.reason;
    }

    private static Probe computeProbe() {
        if (!LINUX) {
            return new Probe(false, "io_uring requires Linux (this JVM reports os.name="
                + System.getProperty("os.name") + ")");
        }
        try {
            Class<?> ioUring = Class.forName(IO_URING_PROBE_CLASS, true, IoUringSupport.class.getClassLoader());
            Method isAvailable = ioUring.getMethod("isAvailable");
            boolean available = (Boolean) isAvailable.invoke(null);
            if (available) {
                return new Probe(true, null);
            }
            String cause = describeUnavailabilityCause(ioUring);
            return new Probe(false, "Netty io_uring transport reports unavailable: " + cause);
        } catch (ClassNotFoundException e) {
            return new Probe(false, "Netty io_uring transport classes not on the classpath ("
                + IO_URING_PROBE_CLASS + ")");
        } catch (ReflectiveOperationException e) {
            LOG.debug("Failed to probe Netty io_uring availability via reflection", e);
            return new Probe(false, "Reflective probe of " + IO_URING_PROBE_CLASS
                + " failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static String describeUnavailabilityCause(Class<?> ioUring) {
        try {
            Method unavailabilityCause = ioUring.getMethod("unavailabilityCause");
            Object cause = unavailabilityCause.invoke(null);
            if (cause == null) return "no cause reported";
            return cause.getClass().getSimpleName() + ": " + ((Throwable) cause).getMessage();
        } catch (ReflectiveOperationException e) {
            return "unavailabilityCause() unreadable (" + e.getClass().getSimpleName() + ")";
        }
    }

    private static final class Probe {
        final boolean available;
        final String reason;
        Probe(boolean available, String reason) {
            this.available = available;
            this.reason = reason;
        }
    }
}
