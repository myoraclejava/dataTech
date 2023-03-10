package com.yunji.lakehouse.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.Iterator;

public class FileSource extends RichSourceFunction<String> implements CheckpointedFunction {

    private BufferedReader br = null;
    private File nextFile = null;
    private ListState<Tuple2<String,Integer>> fileState = null;
    private Tuple2<String,Integer> readOffset = null;
    private boolean isCancel = false;
    private File[] dirs = null;
    private String line = null;
    private int startReadOffset = 0;
    private int readSize = 10;
    private String rootPath = "";
    private int interval = 2000;

    public FileSource(String rootPath, int readSize, int interval) {
        this.rootPath = rootPath;
        this.readSize = readSize;
        this.interval = interval;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        fileState.clear();
        if(readOffset != null)
            fileState.add(readOffset);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor ls = new ListStateDescriptor("sourceFile", Types.TUPLE(Types.STRING,Types.INT));
        fileState = functionInitializationContext.getOperatorStateStore().getListState(ls);
        dirs = new File(rootPath).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.matches("\\d{4}\\-\\d{2}\\-\\d{2}");
            }

        });
        if(functionInitializationContext.isRestored()){
            Iterator<Tuple2<String, Integer>> iterator = fileState.get().iterator();
            if(iterator.hasNext())
                readOffset = iterator.next();
        }
        System.out.println("-----initializeState-----recovered offset: " + (readOffset==null?"null":"("+readOffset.f0+","+readOffset.f1+")"));
        if(readOffset != null) {
            br = new BufferedReader(new FileReader(readOffset.f0));
            for (int i = 0; i < readOffset.f1; i++)
                br.readLine();
        }
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        boolean fileEnd = false;
        long fileModified = 0;
        while(!isCancel){
            if (readOffset != null) {
                startReadOffset = readOffset.f1;
                for (int i = startReadOffset; i < startReadOffset + readSize; i++) {
                    line = br.readLine();
                    if (StringUtils.isEmpty(line)) {
                        fileEnd  = true;
                        fileModified = new File(readOffset.f0).lastModified();
                        System.out.println("-----run-----EOF offset: "+"("+readOffset.f0+","+readOffset.f1+")");
                        break;
                    }
                    System.out.println("-----run-----read offset: "+"("+readOffset.f0+","+String.valueOf(i)+")");
                    sourceContext.collect(line);
                    readOffset = Tuple2.of(readOffset.f0, readOffset.f1 + 1);
                }
            }
            System.out.println("-----run-----sleep interval: "+String.valueOf(interval));
            Thread.sleep(interval);
            if(readOffset == null || fileEnd){
                dirs = new File(rootPath).listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.matches("\\d{4}\\-\\d{2}\\-\\d{2}");
                    }
                });
                for (File dir : dirs)
                    for (File file : dir.listFiles()) {
                        if ((readOffset == null || file.lastModified() > fileModified)
                                && (nextFile == null || file.lastModified() < nextFile.lastModified())) {
                            nextFile = file;
                        }
                    }
                if(nextFile != null && (readOffset == null || !nextFile.getPath().equals(readOffset.f0))){
                    if(br != null)
                        br.close();
                    readOffset = Tuple2.of(nextFile.getPath(),0);
                    br = new BufferedReader(new FileReader(readOffset.f0));
                }
                System.out.println("-----run-----next offset: "+(readOffset==null?"null":"("+readOffset.f0+","+readOffset.f1+")"));
            }
            fileEnd = false;
            fileModified = 0;
            nextFile = null;
        }
    }

    @Override
    public void cancel() {
        isCancel = true;
    }
}
