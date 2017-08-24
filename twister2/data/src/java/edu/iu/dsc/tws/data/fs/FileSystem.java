package edu.iu.dsc.tws.data.fs;

/**
 * This class is an abstract of the generic file system that will be used in the system
 * This can be extend to support distributed file system or a local file system. This defines the basic set of operations
 * that need to be supported by the concrete implementation
 */
public abstract class FileSystem {

    /**
     * Check if the given path exsits
     * @param path
     * @return
     */
    public boolean exists(Path path){
        return true;
    }

    /**
     * Check if file
     * @param path
     * @return
     */
    public boolean isFile(Path path){
        return true;
    }

    /**
     * Check if directory
     * @param path
     * @return
     */
    public boolean isDirectory(Path path){
        return true;
    }

    /**
     * check if isSymlink
     * @param path
     * @return
     */
    public boolean isSymlink(Path path){
        return true;
    }

    /**
     * Set the working Directory
     * @param path
     */
    public abstract void setWorkingDirectory(Path path);

    /**
     * Get the working Directory
     * @return
     */
    public abstract Path getWorkingDirectory();


}
