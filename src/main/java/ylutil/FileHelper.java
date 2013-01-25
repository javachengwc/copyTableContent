package ylutil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileSystemManager;
import org.apache.commons.vfs.VFS;

public class FileHelper {
	// use VFS
	public static void resolvePath(String path) throws Exception {
		FileSystemManager fileSystemManager = VFS.getManager();
		FileObject fileObject = fileSystemManager.resolveFile(path);
		if (fileObject == null) {
			throw new IOException("File cannot be resolved: " + path);
		}
		if (!fileObject.exists()) {
			throw new IOException("File does not exist: " + path);
		}
		URL repoURL = fileObject.getURL();
		if (repoURL == null) {
			throw new Exception("Cannot load connection repository from path: "
					+ path);
		} else {
			System.out.println();
		}
	}

	public static BufferedReader getBufferedReader(String fileName)
			throws FileNotFoundException {
		return new BufferedReader(new FileReader(new File(fileName)));
	}

	public static BufferedWriter getBufferedWriter(String fileName)
			throws IOException {
		return new BufferedWriter(new FileWriter(new File(fileName)));
	}

	/**
	 * 
	 * @param bw
	 */
	public static void closeBW(BufferedWriter bw) {
		if (bw != null) {
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void closeBR(BufferedReader br) {
		if (br != null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * String urlStr = "http://donghua.cntv.cn/zhihuishu/videopage/index.shtml"
	 * 
	 * @return
	 */
	public static BufferedReader getHtmlReader(String htmlUrl) {
		BufferedReader reader = null;
		URL url = null;
		URLConnection conn = null;
		try {
			url = new URL(htmlUrl);
			conn = url.openConnection();
			reader = new BufferedReader(new InputStreamReader(
					conn.getInputStream(), "gbk"));
			return reader;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * after get the inputStream byte[] buffer = new byte[4096]; OutputStream
	 * outstream = new FileOutputStream(new File("file")); while ((len =
	 * is.read(buffer)) > 0) { outstream.write(buffer, 0, len); }
	 * 
	 * @param mp3Address
	 * @return
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	public static InputStream getMp3InputStream(String mp3Address)
			throws MalformedURLException, IOException {
		URLConnection conn = new URL(mp3Address).openConnection();
		return conn.getInputStream();
	}

}
