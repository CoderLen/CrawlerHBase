/**
 * @Author ouyangsuting
 * @Date 2014��9��11������9:48:01
 * @Package org.crawler
 * @Describe
 */
package org.crawler.util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 */
public class DocumentWritable implements WritableComparable<DocumentWritable> {

    private String document;
    private String redirectFrom;
    private boolean metaFollow;
    private boolean metaIndex;

    public DocumentWritable() {
        super();
    }

    public DocumentWritable(String url, String document) {
        super();
        this.redirectFrom = url;
        this.document = document;
        this.metaFollow = MetaParser.CheckMetaFollow(document);
        this.metaIndex = MetaParser.CheckMetaIndex(document);

    }

    public DocumentWritable(String document, String redirectFrom, boolean metaFollow,
                            boolean metaIndex) {
        super();
        this.document = document;
        this.redirectFrom = redirectFrom;
        this.metaFollow = metaFollow;
        this.metaIndex = metaIndex;
    }

    public String getDocument() {
        return document;
    }

    public void setDocument(String document) {
        this.document = document;
    }

    public String getRedirectFrom() {
        return redirectFrom;
    }

    public void setRedirectFrom(String redirectFrom) {
        this.redirectFrom = redirectFrom;
    }

    public boolean getMetaFollow() {
        return metaFollow;
    }

    public void setMetaFollow(boolean metaFollow) {
        this.metaFollow = metaFollow;
    }

    public boolean getMetaIndex() {
        return metaIndex;
    }

    public void setMetaIndex(boolean metaIndex) {
        this.metaIndex = metaIndex;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.document = input.readUTF();
        this.redirectFrom = input.readUTF();
        this.metaFollow = input.readBoolean();
        this.metaIndex = input.readBoolean();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(this.document);
        output.writeUTF(this.redirectFrom);
        output.writeBoolean(this.metaFollow);
        output.writeBoolean(this.metaIndex);
    }

    @Override
    public String toString() {
        return document + "\t" + redirectFrom + "\t" + metaFollow + "\t" + metaIndex;
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(DocumentWritable documentWritable) {
        // TODO Auto-generated method stub
        int cmp = this.document.compareTo(documentWritable.document);
        if (cmp != 0) {
            return cmp;
        }
        int cmp2 = this.redirectFrom.compareTo(documentWritable.redirectFrom);
        if (cmp2 != 0) {
            return cmp2;
        }

        if (this.metaFollow != documentWritable.metaFollow) {
            if (this.metaFollow == false)
                return -1;
            else
                return 1;
        }
        return (this.metaIndex == documentWritable.metaIndex) ? 0 : (this.metaIndex == false) ? -1 : 1;
    }

}
