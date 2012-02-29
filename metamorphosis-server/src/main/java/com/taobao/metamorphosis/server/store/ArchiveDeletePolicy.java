/*
 * (C) 2007-2012 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Authors:
 *   wuhua <wq163@163.com> , boyan <killme2008@gmail.com>
 */
package com.taobao.metamorphosis.server.store;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 归档策略
 * 
 * @author boyan
 * @Date 2011-5-9
 * 
 */
public class ArchiveDeletePolicy extends DiscardDeletePolicy {

    public static final String NAME = "archive";
    private boolean compress = false;
    static final Log log = LogFactory.getLog(ArchiveDeletePolicy.class);


    @Override
    public String name() {
        return NAME;
    }


    boolean isCompress() {
        return this.compress;
    }


    @Override
    public void init(String... values) {
        super.init(values);
        if (values.length >= 2) {
            this.compress = Boolean.valueOf(values[1]);
        }
    }


    /**
     * 归档数据文件
     */
    @Override
    public void process(File file) {
        String name = file.getName();
        if (this.compress) {
            String newName = name.concat(".zip");
            final File newFile = new File(file.getParent(), newName);
            FileOutputStream out = null;
            ZipOutputStream zipOut = null;
            FileInputStream in = null;
            try {
                out = new FileOutputStream(newFile);
                zipOut = new ZipOutputStream(out);
                ZipEntry zipEntry = new ZipEntry(name);
                zipOut.putNextEntry(zipEntry);

                in = new FileInputStream(file);
                byte[] buf = new byte[8192];
                int len = -1;
                while ((len = in.read(buf)) != -1) {
                    zipOut.write(buf, 0, len);
                }
                zipOut.closeEntry();
                zipOut.close();
            }
            catch (IOException e) {
                log.error("Compress file " + file.getAbsolutePath() + " failed", e);
            }
            finally {
                this.close(zipOut);
                this.close(out);
                this.close(in);
                file.delete();
            }
        }
        else {
            String newName = name.concat(".arc");
            final File newFile = new File(file.getParent(), newName);
            file.renameTo(newFile);
        }
    }


    private void close(Closeable out) {
        if (out != null) {
            try {
                out.close();
            }
            catch (IOException e) {

            }
        }
    }

}