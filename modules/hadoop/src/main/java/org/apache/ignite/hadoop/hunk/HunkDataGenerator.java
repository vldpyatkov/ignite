/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.hadoop.hunk;

import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Hunk data generator
 */
public class HunkDataGenerator {
    /**
     *
     * @param args
     * @throws Exception
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void main(String[] args) throws Exception {
        if (args.length < 5)
            throw new Exception("Insufficient arguments!");

        String argSrcFile = args[0];
        String argDstDir = args[1];

        int argTotalSize = Integer.parseInt(args[2]);
        int argFileSize = Integer.parseInt(args[3]);
        int argFilesInFolder = Integer.parseInt(args[4]);

        if (argFilesInFolder <= 0)
            argFilesInFolder = Integer.MAX_VALUE;

        int argFiles = argTotalSize / argFileSize + 1;

        // 0. Cleanup destination dir.
        FileUtils.deleteDirectory(new File(argDstDir));

        // 1. Load source JSON into memory;
        List<String> srcFile = loadSourceFile(argSrcFile);

        int srcFileLineIdx = 0;

        int files = 0;
        int folders = 0;
        int filesInFolder = 0;

        while (true) {
            int curFolder = folders;

            // Create folder if necessary.
            if (filesInFolder == 0) {
                File dir = directory(argDstDir, curFolder);

                dir.mkdirs();

                print("CREATED DIR: " + dir);

                folders++;
            }

            // Create files in folder.
            while (filesInFolder < argFilesInFolder) {
                File file = file(argDstDir, curFolder, filesInFolder);

                try (FileOutputStream fos = new FileOutputStream(file)) {
                    OutputStreamWriter osw = new OutputStreamWriter(fos);
                    BufferedWriter bw = new BufferedWriter(osw);

                    int fileSize = 0;

                    while (fileSize < argFileSize) {
                        int lineIdx = srcFileLineIdx++;

                        if (srcFileLineIdx == srcFile.size())
                            srcFileLineIdx = 0;

                        String line = srcFile.get(lineIdx);

                        line = processLine(line);

                        bw.write(line + "\n");

                        fileSize += line.length();
                    }

                    bw.flush();
                    osw.flush();
                    fos.flush();
                }

                print("CREATED FILE: " + file);

                if (++files > argFiles)
                    return;

                filesInFolder++;
            }

            // Reset files-per-folder counter, so that new folder will be created in the next iteration.
            if (filesInFolder == argFilesInFolder)
                filesInFolder = 0;
        }
    }

    private static String processLine(String line) {
        // TODO: Add attributes w/ different cardinality.
        return line;
    }

    private static File directory(String dstDir, int folderIdx) {
        return new File(dstDir + File.separatorChar + "folder-" + folderIdx);
    }

    private static File file(String dstDir, int folderIdx, int fileIdx) {
        return new File(dstDir + File.separatorChar + "folder-" + folderIdx + File.separatorChar + "file-" + fileIdx);
    }

    private static List<String> loadSourceFile(String path) throws Exception {
        print("LOADING SOURCE DATA ...");

        List<String> res = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(path)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));

            String next = br.readLine();

            while (next != null) {
                res.add(next);

                next = br.readLine();

                if (res.size() % 100_000 == 0)
                    print("---> " + res.size());
            }
        }

        print("---> FINISHED");

        return res;
    }

    private static void print(String data) {
        String time = new Date().toString();

        System.out.println(time + ": " + data);
    }
}
