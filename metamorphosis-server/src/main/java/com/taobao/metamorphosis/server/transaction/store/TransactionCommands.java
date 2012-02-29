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
package com.taobao.metamorphosis.server.transaction.store;

public final class TransactionCommands {
  private TransactionCommands() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public enum TxCommandType
      implements com.google.protobuf.ProtocolMessageEnum {
    APPEND_MSG(0, 1),
    TX_OP(1, 2),
    ;
    
    
    public final int getNumber() { return value; }
    
    public static TxCommandType valueOf(int value) {
      switch (value) {
        case 1: return APPEND_MSG;
        case 2: return TX_OP;
        default: return null;
      }
    }
    
    public static com.google.protobuf.Internal.EnumLiteMap<TxCommandType>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static com.google.protobuf.Internal.EnumLiteMap<TxCommandType>
        internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<TxCommandType>() {
            public TxCommandType findValueByNumber(int number) {
              return TxCommandType.valueOf(number)
    ;        }
          };
    
    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      return getDescriptor().getValues().get(index);
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.getDescriptor().getEnumTypes().get(0);
    }
    
    private static final TxCommandType[] VALUES = {
      APPEND_MSG, TX_OP, 
    };
    public static TxCommandType valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }
    private final int index;
    private final int value;
    private TxCommandType(int index, int value) {
      this.index = index;
      this.value = value;
    }
    
    static {
      com.taobao.metamorphosis.server.transaction.store.TransactionCommands.getDescriptor();
    }
    
    // @@protoc_insertion_point(enum_scope:com.taobao.tddl.dbproxy.protocol.TxCommandType)
  }
  
  public enum TransactionType
      implements com.google.protobuf.ProtocolMessageEnum {
    XA_PREPARE(0, 1),
    XA_COMMIT(1, 2),
    XA_ROLLBACK(2, 3),
    LOCAL_COMMIT(3, 4),
    LOCAL_ROLLBACK(4, 5),
    ;
    
    
    public final int getNumber() { return value; }
    
    public static TransactionType valueOf(int value) {
      switch (value) {
        case 1: return XA_PREPARE;
        case 2: return XA_COMMIT;
        case 3: return XA_ROLLBACK;
        case 4: return LOCAL_COMMIT;
        case 5: return LOCAL_ROLLBACK;
        default: return null;
      }
    }
    
    public static com.google.protobuf.Internal.EnumLiteMap<TransactionType>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static com.google.protobuf.Internal.EnumLiteMap<TransactionType>
        internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<TransactionType>() {
            public TransactionType findValueByNumber(int number) {
              return TransactionType.valueOf(number)
    ;        }
          };
    
    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      return getDescriptor().getValues().get(index);
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.getDescriptor().getEnumTypes().get(1);
    }
    
    private static final TransactionType[] VALUES = {
      XA_PREPARE, XA_COMMIT, XA_ROLLBACK, LOCAL_COMMIT, LOCAL_ROLLBACK, 
    };
    public static TransactionType valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      return VALUES[desc.getIndex()];
    }
    private final int index;
    private final int value;
    private TransactionType(int index, int value) {
      this.index = index;
      this.value = value;
    }
    
    static {
      com.taobao.metamorphosis.server.transaction.store.TransactionCommands.getDescriptor();
    }
    
    // @@protoc_insertion_point(enum_scope:com.taobao.tddl.dbproxy.protocol.TransactionType)
  }
  
  public static final class TxCommand extends
      com.google.protobuf.GeneratedMessage {
    // Use TxCommand.newBuilder() to construct.
    private TxCommand() {
      initFields();
    }
    private TxCommand(boolean noInit) {}
    
    private static final TxCommand defaultInstance;
    public static TxCommand getDefaultInstance() {
      return defaultInstance;
    }
    
    public TxCommand getDefaultInstanceForType() {
      return defaultInstance;
    }
    
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.internal_static_com_taobao_tddl_dbproxy_protocol_TxCommand_descriptor;
    }
    
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.internal_static_com_taobao_tddl_dbproxy_protocol_TxCommand_fieldAccessorTable;
    }
    
    // required .com.taobao.tddl.dbproxy.protocol.TxCommandType cmdType = 1;
    public static final int CMDTYPE_FIELD_NUMBER = 1;
    private boolean hasCmdType;
    private com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommandType cmdType_;
    public boolean hasCmdType() { return hasCmdType; }
    public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommandType getCmdType() { return cmdType_; }
    
    // required bytes cmd_content = 2;
    public static final int CMD_CONTENT_FIELD_NUMBER = 2;
    private boolean hasCmdContent;
    private com.google.protobuf.ByteString cmdContent_ = com.google.protobuf.ByteString.EMPTY;
    public boolean hasCmdContent() { return hasCmdContent; }
    public com.google.protobuf.ByteString getCmdContent() { return cmdContent_; }
    
    // optional bool force = 3 [default = false];
    public static final int FORCE_FIELD_NUMBER = 3;
    private boolean hasForce;
    private boolean force_ = false;
    public boolean hasForce() { return hasForce; }
    public boolean getForce() { return force_; }
    
    private void initFields() {
      cmdType_ = com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommandType.APPEND_MSG;
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input, extensionRegistry)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    
    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }
    
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> {
      private com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand result;
      
      // Construct using com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand.newBuilder()
      private Builder() {}
      
      private static Builder create() {
        Builder builder = new Builder();
        builder.result = new com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand();
        return builder;
      }
      
      protected com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand internalGetResult() {
        return result;
      }
      
      public Builder clear() {
        if (result == null) {
          throw new IllegalStateException(
            "Cannot call clear() after build().");
        }
        result = new com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand();
        return this;
      }
      
      public Builder clone() {
        return create().mergeFrom(result);
      }
      
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand.getDescriptor();
      }
      
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand getDefaultInstanceForType() {
        return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand.getDefaultInstance();
      }
      
      public boolean isInitialized() {
        return result.isInitialized();
      }
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand build() {
        if (result != null && !isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return buildPartial();
      }
      
      private com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand buildParsed()
          throws com.google.protobuf.InvalidProtocolBufferException {
        if (!isInitialized()) {
          throw newUninitializedMessageException(
            result).asInvalidProtocolBufferException();
        }
        return buildPartial();
      }
      
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand buildPartial() {
        if (result == null) {
          throw new IllegalStateException(
            "build() has already been called on this Builder.");
        }
        com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand returnMe = result;
        result = null;
        return returnMe;
      }
      
      
      // required .com.taobao.tddl.dbproxy.protocol.TxCommandType cmdType = 1;
      public boolean hasCmdType() {
        return result.hasCmdType();
      }
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommandType getCmdType() {
        return result.getCmdType();
      }
      public Builder setCmdType(com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommandType value) {
        if (value == null) {
          throw new NullPointerException();
        }
        result.hasCmdType = true;
        result.cmdType_ = value;
        return this;
      }
      public Builder clearCmdType() {
        result.hasCmdType = false;
        result.cmdType_ = com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommandType.APPEND_MSG;
        return this;
      }
      
      // required bytes cmd_content = 2;
      public boolean hasCmdContent() {
        return result.hasCmdContent();
      }
      public com.google.protobuf.ByteString getCmdContent() {
        return result.getCmdContent();
      }
      public Builder setCmdContent(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  result.hasCmdContent = true;
        result.cmdContent_ = value;
        return this;
      }
      public Builder clearCmdContent() {
        result.hasCmdContent = false;
        result.cmdContent_ = getDefaultInstance().getCmdContent();
        return this;
      }
      
      // optional bool force = 3 [default = false];
      public boolean hasForce() {
        return result.hasForce();
      }
      public boolean getForce() {
        return result.getForce();
      }
      public Builder setForce(boolean value) {
        result.hasForce = true;
        result.force_ = value;
        return this;
      }
      public Builder clearForce() {
        result.hasForce = false;
        result.force_ = false;
        return this;
      }
      
      // @@protoc_insertion_point(builder_scope:com.taobao.tddl.dbproxy.protocol.TxCommand)
    }
    
    static {
      defaultInstance = new TxCommand(true);
      com.taobao.metamorphosis.server.transaction.store.TransactionCommands.internalForceInit();
      defaultInstance.initFields();
    }
    
    // @@protoc_insertion_point(class_scope:com.taobao.tddl.dbproxy.protocol.TxCommand)
  }
  
  public static final class AppendMessageCommand extends
      com.google.protobuf.GeneratedMessage {
    // Use AppendMessageCommand.newBuilder() to construct.
    private AppendMessageCommand() {
      initFields();
    }
    private AppendMessageCommand(boolean noInit) {}
    
    private static final AppendMessageCommand defaultInstance;
    public static AppendMessageCommand getDefaultInstance() {
      return defaultInstance;
    }
    
    public AppendMessageCommand getDefaultInstanceForType() {
      return defaultInstance;
    }
    
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.internal_static_com_taobao_tddl_dbproxy_protocol_AppendMessageCommand_descriptor;
    }
    
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.internal_static_com_taobao_tddl_dbproxy_protocol_AppendMessageCommand_fieldAccessorTable;
    }
    
    // required int64 message_id = 1;
    public static final int MESSAGE_ID_FIELD_NUMBER = 1;
    private boolean hasMessageId;
    private long messageId_ = 0L;
    public boolean hasMessageId() { return hasMessageId; }
    public long getMessageId() { return messageId_; }
    
    // required bytes put_command = 2;
    public static final int PUT_COMMAND_FIELD_NUMBER = 2;
    private boolean hasPutCommand;
    private com.google.protobuf.ByteString putCommand_ = com.google.protobuf.ByteString.EMPTY;
    public boolean hasPutCommand() { return hasPutCommand; }
    public com.google.protobuf.ByteString getPutCommand() { return putCommand_; }
    
    private void initFields() {
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input, extensionRegistry)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    
    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }
    
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> {
      private com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand result;
      
      // Construct using com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand.newBuilder()
      private Builder() {}
      
      private static Builder create() {
        Builder builder = new Builder();
        builder.result = new com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand();
        return builder;
      }
      
      protected com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand internalGetResult() {
        return result;
      }
      
      public Builder clear() {
        if (result == null) {
          throw new IllegalStateException(
            "Cannot call clear() after build().");
        }
        result = new com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand();
        return this;
      }
      
      public Builder clone() {
        return create().mergeFrom(result);
      }
      
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand.getDescriptor();
      }
      
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand getDefaultInstanceForType() {
        return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand.getDefaultInstance();
      }
      
      public boolean isInitialized() {
        return result.isInitialized();
      }
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand build() {
        if (result != null && !isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return buildPartial();
      }
      
      private com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand buildParsed()
          throws com.google.protobuf.InvalidProtocolBufferException {
        if (!isInitialized()) {
          throw newUninitializedMessageException(
            result).asInvalidProtocolBufferException();
        }
        return buildPartial();
      }
      
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand buildPartial() {
        if (result == null) {
          throw new IllegalStateException(
            "build() has already been called on this Builder.");
        }
        com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand returnMe = result;
        result = null;
        return returnMe;
      }
      
      
      // required int64 message_id = 1;
      public boolean hasMessageId() {
        return result.hasMessageId();
      }
      public long getMessageId() {
        return result.getMessageId();
      }
      public Builder setMessageId(long value) {
        result.hasMessageId = true;
        result.messageId_ = value;
        return this;
      }
      public Builder clearMessageId() {
        result.hasMessageId = false;
        result.messageId_ = 0L;
        return this;
      }
      
      // required bytes put_command = 2;
      public boolean hasPutCommand() {
        return result.hasPutCommand();
      }
      public com.google.protobuf.ByteString getPutCommand() {
        return result.getPutCommand();
      }
      public Builder setPutCommand(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  result.hasPutCommand = true;
        result.putCommand_ = value;
        return this;
      }
      public Builder clearPutCommand() {
        result.hasPutCommand = false;
        result.putCommand_ = getDefaultInstance().getPutCommand();
        return this;
      }
      
      // @@protoc_insertion_point(builder_scope:com.taobao.tddl.dbproxy.protocol.AppendMessageCommand)
    }
    
    static {
      defaultInstance = new AppendMessageCommand(true);
      com.taobao.metamorphosis.server.transaction.store.TransactionCommands.internalForceInit();
      defaultInstance.initFields();
    }
    
    // @@protoc_insertion_point(class_scope:com.taobao.tddl.dbproxy.protocol.AppendMessageCommand)
  }
  
  public static final class TransactionOperation extends
      com.google.protobuf.GeneratedMessage {
    // Use TransactionOperation.newBuilder() to construct.
    private TransactionOperation() {
      initFields();
    }
    private TransactionOperation(boolean noInit) {}
    
    private static final TransactionOperation defaultInstance;
    public static TransactionOperation getDefaultInstance() {
      return defaultInstance;
    }
    
    public TransactionOperation getDefaultInstanceForType() {
      return defaultInstance;
    }
    
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.internal_static_com_taobao_tddl_dbproxy_protocol_TransactionOperation_descriptor;
    }
    
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.internal_static_com_taobao_tddl_dbproxy_protocol_TransactionOperation_fieldAccessorTable;
    }
    
    // required .com.taobao.tddl.dbproxy.protocol.TransactionType type = 1;
    public static final int TYPE_FIELD_NUMBER = 1;
    private boolean hasType;
    private com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionType type_;
    public boolean hasType() { return hasType; }
    public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionType getType() { return type_; }
    
    // required bool was_prepared = 2;
    public static final int WAS_PREPARED_FIELD_NUMBER = 2;
    private boolean hasWasPrepared;
    private boolean wasPrepared_ = false;
    public boolean hasWasPrepared() { return hasWasPrepared; }
    public boolean getWasPrepared() { return wasPrepared_; }
    
    // required string transaction_id = 3;
    public static final int TRANSACTION_ID_FIELD_NUMBER = 3;
    private boolean hasTransactionId;
    private java.lang.String transactionId_ = "";
    public boolean hasTransactionId() { return hasTransactionId; }
    public java.lang.String getTransactionId() { return transactionId_; }
    
    // optional int32 data_length = 4;
    public static final int DATA_LENGTH_FIELD_NUMBER = 4;
    private boolean hasDataLength;
    private int dataLength_ = 0;
    public boolean hasDataLength() { return hasDataLength; }
    public int getDataLength() { return dataLength_; }
    
    private void initFields() {
      type_ = com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionType.XA_PREPARE;
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return newBuilder().mergeFrom(data, extensionRegistry)
               .buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      Builder builder = newBuilder();
      if (builder.mergeDelimitedFrom(input, extensionRegistry)) {
        return builder.buildParsed();
      } else {
        return null;
      }
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input).buildParsed();
    }
    public static com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return newBuilder().mergeFrom(input, extensionRegistry)
               .buildParsed();
    }
    
    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }
    
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> {
      private com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation result;
      
      // Construct using com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation.newBuilder()
      private Builder() {}
      
      private static Builder create() {
        Builder builder = new Builder();
        builder.result = new com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation();
        return builder;
      }
      
      protected com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation internalGetResult() {
        return result;
      }
      
      public Builder clear() {
        if (result == null) {
          throw new IllegalStateException(
            "Cannot call clear() after build().");
        }
        result = new com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation();
        return this;
      }
      
      public Builder clone() {
        return create().mergeFrom(result);
      }
      
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation.getDescriptor();
      }
      
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation getDefaultInstanceForType() {
        return com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation.getDefaultInstance();
      }
      
      public boolean isInitialized() {
        return result.isInitialized();
      }
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation build() {
        if (result != null && !isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return buildPartial();
      }
      
      private com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation buildParsed()
          throws com.google.protobuf.InvalidProtocolBufferException {
        if (!isInitialized()) {
          throw newUninitializedMessageException(
            result).asInvalidProtocolBufferException();
        }
        return buildPartial();
      }
      
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation buildPartial() {
        if (result == null) {
          throw new IllegalStateException(
            "build() has already been called on this Builder.");
        }
        com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation returnMe = result;
        result = null;
        return returnMe;
      }
      
      
      // required .com.taobao.tddl.dbproxy.protocol.TransactionType type = 1;
      public boolean hasType() {
        return result.hasType();
      }
      public com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionType getType() {
        return result.getType();
      }
      public Builder setType(com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionType value) {
        if (value == null) {
          throw new NullPointerException();
        }
        result.hasType = true;
        result.type_ = value;
        return this;
      }
      public Builder clearType() {
        result.hasType = false;
        result.type_ = com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionType.XA_PREPARE;
        return this;
      }
      
      // required bool was_prepared = 2;
      public boolean hasWasPrepared() {
        return result.hasWasPrepared();
      }
      public boolean getWasPrepared() {
        return result.getWasPrepared();
      }
      public Builder setWasPrepared(boolean value) {
        result.hasWasPrepared = true;
        result.wasPrepared_ = value;
        return this;
      }
      public Builder clearWasPrepared() {
        result.hasWasPrepared = false;
        result.wasPrepared_ = false;
        return this;
      }
      
      // required string transaction_id = 3;
      public boolean hasTransactionId() {
        return result.hasTransactionId();
      }
      public java.lang.String getTransactionId() {
        return result.getTransactionId();
      }
      public Builder setTransactionId(java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  result.hasTransactionId = true;
        result.transactionId_ = value;
        return this;
      }
      public Builder clearTransactionId() {
        result.hasTransactionId = false;
        result.transactionId_ = getDefaultInstance().getTransactionId();
        return this;
      }
      
      // optional int32 data_length = 4;
      public boolean hasDataLength() {
        return result.hasDataLength();
      }
      public int getDataLength() {
        return result.getDataLength();
      }
      public Builder setDataLength(int value) {
        result.hasDataLength = true;
        result.dataLength_ = value;
        return this;
      }
      public Builder clearDataLength() {
        result.hasDataLength = false;
        result.dataLength_ = 0;
        return this;
      }
      
      // @@protoc_insertion_point(builder_scope:com.taobao.tddl.dbproxy.protocol.TransactionOperation)
    }
    
    static {
      defaultInstance = new TransactionOperation(true);
      com.taobao.metamorphosis.server.transaction.store.TransactionCommands.internalForceInit();
      defaultInstance.initFields();
    }
    
    // @@protoc_insertion_point(class_scope:com.taobao.tddl.dbproxy.protocol.TransactionOperation)
  }
  
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_taobao_tddl_dbproxy_protocol_TxCommand_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_taobao_tddl_dbproxy_protocol_TxCommand_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_taobao_tddl_dbproxy_protocol_AppendMessageCommand_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_taobao_tddl_dbproxy_protocol_AppendMessageCommand_fieldAccessorTable;
  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_com_taobao_tddl_dbproxy_protocol_TransactionOperation_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_com_taobao_tddl_dbproxy_protocol_TransactionOperation_fieldAccessorTable;
  
  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\031TransactionCommands.proto\022 com.taobao." +
      "tddl.dbproxy.protocol\"x\n\tTxCommand\022@\n\007cm" +
      "dType\030\001 \002(\0162/.com.taobao.tddl.dbproxy.pr" +
      "otocol.TxCommandType\022\023\n\013cmd_content\030\002 \002(" +
      "\014\022\024\n\005force\030\003 \001(\010:\005false\"?\n\024AppendMessage" +
      "Command\022\022\n\nmessage_id\030\001 \002(\003\022\023\n\013put_comma" +
      "nd\030\002 \002(\014\"\232\001\n\024TransactionOperation\022?\n\004typ" +
      "e\030\001 \002(\01621.com.taobao.tddl.dbproxy.protoc" +
      "ol.TransactionType\022\024\n\014was_prepared\030\002 \002(\010" +
      "\022\026\n\016transaction_id\030\003 \002(\t\022\023\n\013data_length\030",
      "\004 \001(\005**\n\rTxCommandType\022\016\n\nAPPEND_MSG\020\001\022\t" +
      "\n\005TX_OP\020\002*g\n\017TransactionType\022\016\n\nXA_PREPA" +
      "RE\020\001\022\r\n\tXA_COMMIT\020\002\022\017\n\013XA_ROLLBACK\020\003\022\020\n\014" +
      "LOCAL_COMMIT\020\004\022\022\n\016LOCAL_ROLLBACK\020\005BJ\n1co" +
      "m.taobao.metamorphosis.server.transactio" +
      "n.storeB\023TransactionCommandsH\002"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_com_taobao_tddl_dbproxy_protocol_TxCommand_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_com_taobao_tddl_dbproxy_protocol_TxCommand_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_taobao_tddl_dbproxy_protocol_TxCommand_descriptor,
              new java.lang.String[] { "CmdType", "CmdContent", "Force", },
              com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand.class,
              com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TxCommand.Builder.class);
          internal_static_com_taobao_tddl_dbproxy_protocol_AppendMessageCommand_descriptor =
            getDescriptor().getMessageTypes().get(1);
          internal_static_com_taobao_tddl_dbproxy_protocol_AppendMessageCommand_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_taobao_tddl_dbproxy_protocol_AppendMessageCommand_descriptor,
              new java.lang.String[] { "MessageId", "PutCommand", },
              com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand.class,
              com.taobao.metamorphosis.server.transaction.store.TransactionCommands.AppendMessageCommand.Builder.class);
          internal_static_com_taobao_tddl_dbproxy_protocol_TransactionOperation_descriptor =
            getDescriptor().getMessageTypes().get(2);
          internal_static_com_taobao_tddl_dbproxy_protocol_TransactionOperation_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_com_taobao_tddl_dbproxy_protocol_TransactionOperation_descriptor,
              new java.lang.String[] { "Type", "WasPrepared", "TransactionId", "DataLength", },
              com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation.class,
              com.taobao.metamorphosis.server.transaction.store.TransactionCommands.TransactionOperation.Builder.class);
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }
  
  public static void internalForceInit() {}
  
  // @@protoc_insertion_point(outer_class_scope)
}