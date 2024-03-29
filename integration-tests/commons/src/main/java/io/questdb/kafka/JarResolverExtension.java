package io.questdb.kafka;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Objects;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public final class JarResolverExtension implements Extension, AfterAllCallback {
    private final Class<?> clazz;
    private Path tempDir;

    private JarResolverExtension(Class<?> clazz) {
        this.clazz = clazz;
    }

    public static JarResolverExtension forClass(Class<?> clazz) {
        return new JarResolverExtension(clazz);
    }

    public String getJarPath() {
        URL resource = this.getClass().getClassLoader().getResource(clazz.getName().replace(".", "/") + ".class");
        if (resource == null) {
            throw new IllegalStateException("Could not find class " + clazz.getName() + " in classpath");
        }
        if (resource.getProtocol().equals("file")) {
            String pathString = resource.getPath();
            return buildJarFromSiblingTargetDir(pathString);
        } else if (resource.getProtocol().equals("jar")) {
            return getPathToJarWithClass(clazz);
        }
        throw new UnsupportedOperationException("Unsupported classpath entry protocol: " + resource);
    }

    private static String getPathToJarWithClass(Class<?> clazz) {
        URL resource = clazz.getClassLoader().getResource(clazz.getName().replace(".", "/") + ".class");
        String stringPath = Objects.requireNonNull(resource, "class " + clazz + " not found").getPath();
        stringPath = stringPath.substring("file:".length(), stringPath.indexOf("!"));
        Path path = Paths.get(stringPath);
        return path.toString();
    }

    private String buildJarFromSiblingTargetDir(String pathString) {
        try {
            tempDir = Files.createTempDirectory("jar-resolver-tmp");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Path path = Paths.get(pathString);
        do {
            path = path.getParent();
        } while (!path.getFileName().toString().equals("classes"));
        return createConnectorJar(path.toString());
    }

    private String createConnectorJar(String basePath) {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        File output = new File(tempDir.toFile(), clazz.getSimpleName() + ".jar");
        JarOutputStream target;
        try {
            target = new JarOutputStream(Files.newOutputStream(output.toPath()), manifest);
            add(basePath, new File(basePath), target);
            target.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return output.getAbsolutePath();
    }

    private static void add(String basePath, File source, JarOutputStream target) throws IOException {
        String name = source.getPath().replace("\\", "/").replace(basePath, "");
        if (name.startsWith("/")) {
            name = name.substring(1);
        }

        if (source.isDirectory()) {
            if (!name.endsWith("/")) {
                name += "/";
            }
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            target.closeEntry();
            for (File nestedFile : Objects.requireNonNull(source.listFiles())) {
                add(basePath, nestedFile, target);
            }
        }
        else {
            JarEntry entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(source.toPath()))) {
                byte[] buffer = new byte[1024];
                while (true) {
                    int count = in.read(buffer);
                    if (count == -1)
                        break;
                    target.write(buffer, 0, count);
                }
                target.closeEntry();
            }
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (tempDir != null) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }
}
