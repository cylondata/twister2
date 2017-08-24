packageedu.iu.dsc.tws.data.fs;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Used to name tha files in the FileSystem
 */
public class Path implements Serializable {

    /**
     * The directory seperator, a slash
     */
    public static final String SEPARATOR = "/";

    /**
     * The directory separator, a slash as char
     */
    public static final char SEPARATOR_CHAR = '/';

    /**
     * The current directory
     */
    public static final String CUR_DIR = ".";

    private URI uri;

    /**
     * Empty constructor
     */
    public Path() {}

    /**
     * Create Path with given URI
     * @param uri
     */
    public Path(URI uri) {this.uri = uri;}

    /**
     * create Path with parent and child
     * @param parent
     * @param child
     */
    public Path(String parent, String child) {
        this(new Path(parent), new Path(child));
    }

    /**
     *
     * @param parent
     * @param child
     */
    public Path(String parent, Path child) {
        this(new Path(parent), child);
    }

    public Path(Path parent, Path child) {
        // Add a slash to parent's path so resolution is compatible with URI's
        URI parentUri = parent.uri;
        final String parentPath = parentUri.getPath();
        if (!(parentPath.equals("/") || parentPath.equals(""))) {
            try {
                parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(), parentUri.getPath() + "/", null,
                        null);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }

        if (child.uri.getPath().startsWith(Path.SEPARATOR)) {
            child = new Path(child.uri.getScheme(), child.uri.getAuthority(), child.uri.getPath().substring(1));
        }

        final URI resolved = parentUri.resolve(child.uri);
        initialize(resolved.getScheme(), resolved.getAuthority(), normalizePath(resolved.getPath()));
    }


    public Path(String scheme, String authority, String path) {
        path = checkAndTrimPathArg(path);
        initialize(scheme, authority, path);
    }

    /**
     * Create path from given path String
     * @param pathString
     */
    public Path(String pathString) {
        pathString = checkAndTrimPathArg(pathString);

        // We can't use 'new URI(String)' directly, since it assumes things are
        // escaped, which we don't require of Paths.

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(pathString, false)) {
            pathString = "/" + pathString;
        }

        // parse uri components
        String scheme = null;
        String authority = null;

        int start = 0;

        // parse uri scheme, if any
        final int colon = pathString.indexOf(':');
        final int slash = pathString.indexOf('/');
        if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a
            // scheme
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start) && (pathString.length() - start > 2)) { // has authority
            final int nextSlash = pathString.indexOf('/', start + 2);
            final int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        // uri path is the rest of the string -- query & fragment not supported
        final String path = pathString.substring(start, pathString.length());

        initialize(scheme, authority, path);
    }


    /**
     *
     * @param scheme
     * @param authority
     * @param path
     */
    private void initialize(String scheme, String authority, String path) {
        try {
            this.uri = new URI(scheme, authority, normalizePath(path), null, null).normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private String normalizePath(String path) {

        // remove leading and tailing whitespaces
        path = path.trim();

        // remove consecutive slashes & backslashes
        path = path.replace("\\", "/");
        path = path.replaceAll("/+", "/");

        // remove tailing separator
        if(!path.equals(SEPARATOR) &&         		// UNIX root path
                !path.matches("/\\p{Alpha}+:/") &&  // Windows root path
                path.endsWith(SEPARATOR))
        {
            // remove tailing slash
            path = path.substring(0, path.length() - SEPARATOR.length());
        }

        return path;
    }

    private String checkAndTrimPathArg(String path) {
        // disallow construction of a Path from an empty string
        if (path == null) {
            throw new IllegalArgumentException("Can not create a Path from a null string");
        }
        path = path.trim();
        if (path.length() == 0) {
            throw new IllegalArgumentException("Can not create a Path from an empty string");
        }
        return path;
    }

    /**
     * Checks if the provided path string contains a windows drive letter.
     *
     * @param path
     *        the path to check
     * @param slashed
     *         true to indicate the first character of the string is a slash, false otherwise
     *
     * @return <code>true</code> if the path string contains a windows drive letter, false otherwise
     */
    private boolean hasWindowsDrive(String path, boolean slashed) {
        final int start = slashed ? 1 : 0;
        return path.length() >= start + 2
                && (!slashed || path.charAt(0) == '/')
                && path.charAt(start + 1) == ':'
                && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z') || (path.charAt(start) >= 'a' && path
                .charAt(start) <= 'z'));
    }

    /**
     * get full path.
     * @return full path
     */
    public String getPath() {
        return uri.getPath();
    }

    public Path getParent() {
        final String path = uri.getPath();
        final int lastSlash = path.lastIndexOf('/');
        final int start = hasWindowsDrive(path, true) ? 3 : 0;
        if ((path.length() == start) || // empty path
                (lastSlash == start && path.length() == start + 1)) { // at root
            return null;
        }
        String parent;
        if (lastSlash == -1) {
            parent = CUR_DIR;
        } else {
            final int end = hasWindowsDrive(path, true) ? 3 : 0;
            parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);
        }
        return new Path(uri.getScheme(), uri.getAuthority(), parent);
    }

    @Override
    public String toString() {
        // we can't use uri.toString(), which escapes everything, because we want
        // illegal characters unescaped in the string, for glob processing, etc.
        final StringBuilder buffer = new StringBuilder();
        if (uri.getScheme() != null) {
            buffer.append(uri.getScheme());
            buffer.append(":");
        }
        if (uri.getAuthority() != null) {
            buffer.append("//");
            buffer.append(uri.getAuthority());
        }
        if (uri.getPath() != null) {
            String path = uri.getPath();
            if (path.indexOf('/') == 0 && hasWindowsDrive(path, true) && // has windows drive
                    uri.getScheme() == null && // but no scheme
                    uri.getAuthority() == null) { // or authority
                path = path.substring(1); // remove slash before drive
            }
            buffer.append(path);
        }
        return buffer.toString();
    }
}
