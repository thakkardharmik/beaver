/**
* Copyright  (c) 2011-2016, Hortonworks Inc.  All rights reserved.
*
* Except as expressly permitted in a written agreement between your 
* company and Hortonworks, Inc, any use, reproduction, modification, 
* redistribution, sharing, lending or other exploitation of all or 
* any part of the contents of this file is strictly prohibited.
*
*/
package com.hortonworks.qe.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;

/**
 * Write specified number of words to files.
 * Number of unique words are number of words in word set.
 * If numUniqueWords is 1,000, word set is [word00000, word00999]. (word shown has reduced leading zeros.).
 * word00000 can appear more than 1 times.
 * Each word occupies one line.
 * @author tathiapinya
 *
 */
public class CustomWordWriter {
    
    private static final int NUM_DIGITS_FILE = 5;
    private static final int NUM_DIGITS_WORD = 12;
    private String dir;
    private int numFiles;
    private int numUniqueWords;
    private int numWordsPerFile;
    private Random randomGen;
    
    public CustomWordWriter(String dir, int numFiles, int numUniqueWords, int numWordsPerFile) {
        this.dir = dir;
        this.numFiles = numFiles;
        this.numUniqueWords = numUniqueWords;
        this.numWordsPerFile = numWordsPerFile;
        randomGen = new Random(System.currentTimeMillis());
    }
    
    public String getWord() {
        int num = (int) Math.round(randomGen.nextDouble() * numUniqueWords);
        return "word" + getLeadingZeroesString(num, NUM_DIGITS_WORD);
    }
    
    private String getLeadingZeroesString(int num, int totalLengthNeeded) {
        String result = "";
        String numS = Integer.toString(num);
        if (numS.length() < totalLengthNeeded) {
            for (int i=0; i < totalLengthNeeded - numS.length(); i++)
                result = result + "0";
        }
        result = result + numS;
        return result;
    }
    public void genFiles() {
        File fDir = new File(dir);
        fDir.mkdirs();
        for (int fileNo=0; fileNo < numFiles; fileNo++) {
            try  
            {
                String fileNoStr = getLeadingZeroesString(fileNo + 1, NUM_DIGITS_FILE);
                FileWriter fstream = new FileWriter(dir + File.separator + "file" + (fileNoStr) + ".txt", false); 
                BufferedWriter out = new BufferedWriter(fstream);
                for (int wordNo=0; wordNo < numWordsPerFile; wordNo++) {
                    out.write(getWord());
                    out.newLine();
                }
                out.close();
            }
            catch (Exception e)
            {
                System.err.println("Error: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: CustomWordWriter <dir> <numFiles> <numUniqueWords per file> <numWords per file>");
            System.exit(0);
        } else {
            String dir = args[0];
            int numFiles = Integer.parseInt(args[1]);
            int numUniqueWords = Integer.parseInt(args[2]);
            int numWord = Integer.parseInt(args[3]);
            CustomWordWriter c = new CustomWordWriter(dir, numFiles, numUniqueWords, 
                                                      numWord);
            c.genFiles();
            File dirFileObj = new File(dir);
            System.out.println("Done. Wrote " + numFiles + " files to " + dirFileObj.getAbsolutePath() + ".");
            System.out.println("Number of global unique words = " + numUniqueWords);
            System.out.println("Number of words per file = " + numWord);
        }
        
    }

}
