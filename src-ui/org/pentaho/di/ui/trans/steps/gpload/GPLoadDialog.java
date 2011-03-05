 /* Copyright (c) 2007 Pentaho Corporation.  All rights reserved. 
 * This software was developed by Pentaho Corporation and is provided under the terms 
 * of the GNU Lesser General Public License, Version 2.1. You may not use 
 * this file except in compliance with the license. If you need a copy of the license, 
 * please go to http://www.gnu.org/licenses/lgpl-2.1.txt. The Original Code is Pentaho 
 * Data Integration.  The Initial Developer is Pentaho Corporation.
 *
 * Software distributed under the GNU Lesser Public License is distributed on an "AS IS" 
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or  implied. Please refer to 
 * the license for the specific language governing your rights and limitations.*/

package org.pentaho.di.ui.trans.steps.gpload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.SQLStatement;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.gpload.GPLoadMeta;
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.database.dialog.SQLEditor;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;


/**
 * Dialog class for the Greenplum bulk loader step.
 * Created on 28mar2008, copied from Sven Boden's Oracle version
 * 
 * @author Luke Lonergan
 */
public class GPLoadDialog extends BaseStepDialog implements StepDialogInterface
{
	private static Class<?> PKG = GPLoadMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

	private CCombo				wConnection;

    private Label               wlSchema;
    private TextVar             wSchema;
    private FormData            fdlSchema, fdSchema;

	private Label				wlTable;
	private Button				wbTable;
	private TextVar				wTable;
	private FormData			fdlTable, fdbTable, fdTable;
	
   private Label           wlErrorTable;
   private Button          wbErrorTable;
   private TextVar         wErrorTable;
   private FormData        fdlErrorTable, fdbErrorTable, fdErrorTable;

	private Label				wlGploadPath;
	private Button				wbGploadPath;
	private TextVar				wGploadPath;
	private FormData			fdlGploadPath, fdbGploadPath, fdGploadPath;
	
	private Label               wlLoadMethod;
	private CCombo              wLoadMethod;
	private FormData            fdlLoadMethod, fdLoadMethod;	

	private Label               wlLoadAction;
	private CCombo              wLoadAction;
	private FormData            fdlLoadAction, fdLoadAction;		

	private Label				wlMaxErrors;
	private TextVar				wMaxErrors;
	private FormData			fdlMaxErrors, fdMaxErrors;

    // private FormData fdReadSize;
	
	private Label				wlReturn;
	private TableView			wReturn;
	private FormData			fdlReturn, fdReturn;

	private Label				wlControlFile;
	private Button				wbControlFile;
	private TextVar				wControlFile;
	private FormData			fdlControlFile, fdbControlFile, fdControlFile;

	private Label				wlDataFile;
	private Button				wbDataFile;
	private TextVar				wDataFile;
	private FormData			fdlDataFile, fdbDataFile, fdDataFile;	

	private Label				wlLogFile;
	private Button				wbLogFile;
	private TextVar				wLogFile;
	private FormData			fdlLogFile, fdbLogFile, fdLogFile;

    private Label				wlDbNameOverride;
	private TextVar				wDbNameOverride;
	private FormData			fdlDbNameOverride, fdDbNameOverride;		
		
    private Label               wlEncoding;
    private Combo               wEncoding;
    private FormData            fdlEncoding, fdEncoding;

    private Label				wlEraseFiles;
	private Button				wEraseFiles;
	private FormData			fdlEraseFiles, fdEraseFiles;		
	
	private Button				wGetLU;
	private FormData			fdGetLU;
	private Listener			lsGetLU;
	
	private Button     wDoMapping;
	private FormData   fdDoMapping;

	private GPLoadMeta	input;
	
	/**
	 * List of ColumnInfo that should have the field names of the selected database table
	 */
	private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();
	
	private ColumnInfo[] ciReturn ;
	
    // These should not be translated, they are required to exist on all
    // platforms according to the documentation of "Charset".
    private static String[] encodings = { "",                //$NON-NLS-1$
    	                                  "US-ASCII",        //$NON-NLS-1$
    	                                  "ISO-8859-1",      //$NON-NLS-1$
    	                                  "UTF-8",           //$NON-NLS-1$
    	                                  "UTF-16BE",        //$NON-NLS-1$
    	                                  "UTF-16LE",        //$NON-NLS-1$
    	                                  "UTF-16" };        //$NON-NLS-1$

    private static final String[] ALL_FILETYPES = new String[] {
        	BaseMessages.getString(PKG, "GPLoadDialog.Filetype.All") };

    private Map<String, Integer> inputFields;

	public GPLoadDialog(Shell parent, Object in, TransMeta transMeta, String sname)
	{
		super(parent, (BaseStepMeta)in, transMeta, sname);
		input = (GPLoadMeta) in;
        inputFields =new HashMap<String, Integer>();
	}

	public String open()
	{
		Shell parent = getParent();
		Display display = parent.getDisplay();

		shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
 		props.setLook(shell);
        setShellImage(shell, input);

		ModifyListener lsMod = new ModifyListener()
		{
			public void modifyText(ModifyEvent e)
			{
				input.setChanged();
			}
		};
		FocusListener lsFocusLost = new FocusAdapter() {
			public void focusLost(FocusEvent arg0) {
				setTableFieldCombo();
			}
		};
		changed = input.hasChanged();

		FormLayout formLayout = new FormLayout();
		formLayout.marginWidth = Const.FORM_MARGIN;
		formLayout.marginHeight = Const.FORM_MARGIN;

		shell.setLayout(formLayout);
		shell.setText(BaseMessages.getString(PKG, "GPLoadDialog.Shell.Title")); //$NON-NLS-1$

		int middle = props.getMiddlePct();
		int margin = Const.MARGIN;

		// Stepname line
		wlStepname = new Label(shell, SWT.RIGHT);
		wlStepname.setText(BaseMessages.getString(PKG, "GPLoadDialog.Stepname.Label")); //$NON-NLS-1$
 		props.setLook(wlStepname);
		fdlStepname = new FormData();
		fdlStepname.left = new FormAttachment(0, 0);
		fdlStepname.right = new FormAttachment(middle, -margin);
		fdlStepname.top = new FormAttachment(0, margin);
		wlStepname.setLayoutData(fdlStepname);
		wStepname = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
		wStepname.setText(stepname);
 		props.setLook(wStepname);
		wStepname.addModifyListener(lsMod);
		fdStepname = new FormData();
		fdStepname.left = new FormAttachment(middle, 0);
		fdStepname.top = new FormAttachment(0, margin);
		fdStepname.right = new FormAttachment(100, 0);
		wStepname.setLayoutData(fdStepname);

		// Connection line
		wConnection = addConnectionLine(shell, wStepname, middle, margin);
		if (input.getDatabaseMeta()==null && transMeta.nrDatabases()==1) wConnection.select(0);
		wConnection.addModifyListener(lsMod);

        // Schema line...
        wlSchema=new Label(shell, SWT.RIGHT);
        wlSchema.setText(BaseMessages.getString(PKG, "GPLoadDialog.TargetSchema.Label")); //$NON-NLS-1$
        props.setLook(wlSchema);
        fdlSchema=new FormData();
        fdlSchema.left = new FormAttachment(0, 0);
        fdlSchema.right= new FormAttachment(middle, -margin);
        fdlSchema.top  = new FormAttachment(wConnection, margin*2);
        wlSchema.setLayoutData(fdlSchema);

        wSchema=new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(wSchema);
        wSchema.addModifyListener(lsMod);
        wSchema.addFocusListener(lsFocusLost);
        fdSchema=new FormData();
        fdSchema.left = new FormAttachment(middle, 0);
        fdSchema.top  = new FormAttachment(wConnection, margin*2);
        fdSchema.right= new FormAttachment(100, 0);
        wSchema.setLayoutData(fdSchema);

		// Table line...
		wlTable = new Label(shell, SWT.RIGHT);
		wlTable.setText(BaseMessages.getString(PKG, "GPLoadDialog.TargetTable.Label")); //$NON-NLS-1$
 		props.setLook(wlTable);
		fdlTable = new FormData();
		fdlTable.left = new FormAttachment(0, 0);
		fdlTable.right = new FormAttachment(middle, -margin);
		fdlTable.top = new FormAttachment(wSchema, margin);
		wlTable.setLayoutData(fdlTable);
		
		wbTable = new Button(shell, SWT.PUSH | SWT.CENTER);
 		props.setLook(wbTable);
		wbTable.setText(BaseMessages.getString(PKG, "GPLoadDialog.Browse.Button")); //$NON-NLS-1$
		fdbTable = new FormData();
		fdbTable.right = new FormAttachment(100, 0);
		fdbTable.top = new FormAttachment(wSchema, margin);
		wbTable.setLayoutData(fdbTable);
		wTable = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wTable);
		wTable.addModifyListener(lsMod);
		wTable.addFocusListener(lsFocusLost);
		fdTable = new FormData();
		fdTable.left = new FormAttachment(middle, 0);
		fdTable.top = new FormAttachment(wSchema, margin);
		fdTable.right = new FormAttachment(wbTable, -margin);
		wTable.setLayoutData(fdTable);

		// GPLoad line...
		wlGploadPath = new Label(shell, SWT.RIGHT);
		wlGploadPath.setText(BaseMessages.getString(PKG, "GPLoadDialog.GPLoadPath.Label")); //$NON-NLS-1$
 		props.setLook(wlGploadPath);
		fdlGploadPath = new FormData();
		fdlGploadPath.left = new FormAttachment(0, 0);
		fdlGploadPath.right = new FormAttachment(middle, -margin);
		fdlGploadPath.top = new FormAttachment(wTable, margin);
		wlGploadPath.setLayoutData(fdlGploadPath);
		
		wbGploadPath = new Button(shell, SWT.PUSH | SWT.CENTER);
 		props.setLook(wbGploadPath);
		wbGploadPath.setText(BaseMessages.getString(PKG, "GPLoadDialog.Browse.Button")); //$NON-NLS-1$
		fdbGploadPath = new FormData();
		fdbGploadPath.right = new FormAttachment(100, 0);
		fdbGploadPath.top = new FormAttachment(wTable, margin);
		wbGploadPath.setLayoutData(fdbGploadPath);
		wGploadPath = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wGploadPath);
		wGploadPath.addModifyListener(lsMod);
		fdGploadPath = new FormData();
		fdGploadPath.left = new FormAttachment(middle, 0);
		fdGploadPath.top = new FormAttachment(wTable, margin);
		fdGploadPath.right = new FormAttachment(wbGploadPath, -margin);
		wGploadPath.setLayoutData(fdGploadPath);
				
		// Load Method line
		wlLoadMethod = new Label(shell, SWT.RIGHT);
		wlLoadMethod.setText(BaseMessages.getString(PKG, "GPLoadDialog.LoadMethod.Label"));
		props.setLook(wlLoadMethod);
		fdlLoadMethod = new FormData();
		fdlLoadMethod.left = new FormAttachment(0, 0);
		fdlLoadMethod.right = new FormAttachment(middle, -margin);
		fdlLoadMethod.top = new FormAttachment(wGploadPath, margin);
		wlLoadMethod.setLayoutData(fdlLoadMethod);
		wLoadMethod = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
		//wLoadMethod.add(BaseMessages.getString(PKG, "GPLoadDialog.AutoConcLoadMethod.Label"));
		wLoadMethod.add(BaseMessages.getString(PKG, "GPLoadDialog.AutoEndLoadMethod.Label"));
		wLoadMethod.add(BaseMessages.getString(PKG, "GPLoadDialog.ManualLoadMethod.Label"));
		wLoadMethod.select(0); // +1: starts at -1
		wLoadMethod.addModifyListener(lsMod);
		
		props.setLook(wLoadMethod);
		fdLoadMethod= new FormData();
		fdLoadMethod.left = new FormAttachment(middle, 0);
		fdLoadMethod.top = new FormAttachment(wGploadPath, margin);
		fdLoadMethod.right = new FormAttachment(100, 0);
		wLoadMethod.setLayoutData(fdLoadMethod);
		
		fdLoadMethod = new FormData();
		fdLoadMethod.left = new FormAttachment(middle, 0);
		fdLoadMethod.top = new FormAttachment(wGploadPath, margin);
		fdLoadMethod.right = new FormAttachment(100, 0);
		wLoadMethod.setLayoutData(fdLoadMethod);					
		
		// Load Action line
		wlLoadAction = new Label(shell, SWT.RIGHT);
		wlLoadAction.setText(BaseMessages.getString(PKG, "GPLoadDialog.LoadAction.Label"));
		props.setLook(wlLoadAction);
		fdlLoadAction = new FormData();
		fdlLoadAction.left = new FormAttachment(0, 0);
		fdlLoadAction.right = new FormAttachment(middle, -margin);
		fdlLoadAction.top = new FormAttachment(wLoadMethod, margin);
		wlLoadAction.setLayoutData(fdlLoadAction);
		wLoadAction = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
		wLoadAction.add(BaseMessages.getString(PKG, "GPLoadDialog.InsertLoadAction.Label"));
        wLoadAction.add(BaseMessages.getString(PKG, "GPLoadDialog.UpdateLoadAction.Label"));
		wLoadAction.add(BaseMessages.getString(PKG, "GPLoadDialog.MergeLoadAction.Label"));
		
		wLoadAction.select(0); // +1: starts at -1
		wLoadAction.addModifyListener(lsMod);
		
		props.setLook(wLoadAction);
		fdLoadAction= new FormData();
		fdLoadAction.left = new FormAttachment(middle, 0);
		fdLoadAction.top = new FormAttachment(wLoadMethod, margin);
		fdLoadAction.right = new FormAttachment(100, 0);
		wLoadAction.setLayoutData(fdLoadAction);
		
		fdLoadAction = new FormData();
		fdLoadAction.left = new FormAttachment(middle, 0);
		fdLoadAction.top = new FormAttachment(wLoadMethod, margin);
		fdLoadAction.right = new FormAttachment(100, 0);
		wLoadAction.setLayoutData(fdLoadAction);				

		// MaxErrors file line
		wlMaxErrors = new Label(shell, SWT.RIGHT);
		wlMaxErrors.setText(BaseMessages.getString(PKG, "GPLoadDialog.MaxErrors.Label")); //$NON-NLS-1$
 		props.setLook(wlMaxErrors);
		fdlMaxErrors = new FormData();
		fdlMaxErrors.left = new FormAttachment(0, 0);
		fdlMaxErrors.top = new FormAttachment(wLoadAction, margin);
		fdlMaxErrors.right = new FormAttachment(middle, -margin);
		wlMaxErrors.setLayoutData(fdlMaxErrors);
		wMaxErrors = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wMaxErrors);
		wMaxErrors.addModifyListener(lsMod);
		fdMaxErrors = new FormData();
		fdMaxErrors.left = new FormAttachment(middle, 0);
		fdMaxErrors.top = new FormAttachment(wLoadAction, margin);
		fdMaxErrors.right = new FormAttachment(100, 0);
		wMaxErrors.setLayoutData(fdMaxErrors);						

	    // Error Table line...
      wlErrorTable = new Label(shell, SWT.RIGHT);
      wlErrorTable.setText(BaseMessages.getString(PKG, "GPLoadDialog.ErrorTable.Label")); //$NON-NLS-1$
      props.setLook(wlErrorTable);
      fdlErrorTable = new FormData();
      fdlErrorTable.left = new FormAttachment(0, 0);
      fdlErrorTable.right = new FormAttachment(middle, -margin);
      fdlErrorTable.top = new FormAttachment(wMaxErrors, margin);
      wlErrorTable.setLayoutData(fdlErrorTable);
      
      wbErrorTable = new Button(shell, SWT.PUSH | SWT.CENTER);
      props.setLook(wbErrorTable);
      wbErrorTable.setText(BaseMessages.getString(PKG, "GPLoadDialog.Browse.Button")); //$NON-NLS-1$
      fdbErrorTable = new FormData();
      fdbErrorTable.right = new FormAttachment(100, 0);
      fdbErrorTable.top = new FormAttachment(wMaxErrors, margin);
      wbErrorTable.setLayoutData(fdbErrorTable);
      wErrorTable = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      props.setLook(wErrorTable);
      wErrorTable.addModifyListener(lsMod);
      wErrorTable.addFocusListener(lsFocusLost);
      fdErrorTable = new FormData();
      fdErrorTable.left = new FormAttachment(middle, 0);
      fdErrorTable.top = new FormAttachment(wMaxErrors, margin);
      fdErrorTable.right = new FormAttachment(wbErrorTable, -margin);
      wErrorTable.setLayoutData(fdErrorTable);
		
		
		// Db Name Override line
		wlDbNameOverride = new Label(shell, SWT.RIGHT);
		wlDbNameOverride.setText(BaseMessages.getString(PKG, "GPLoadDialog.DbNameOverride.Label")); //$NON-NLS-1$
 		props.setLook(wlDbNameOverride);
		fdlDbNameOverride = new FormData();
		fdlDbNameOverride.left = new FormAttachment(0, 0);
		fdlDbNameOverride.top = new FormAttachment(wMaxErrors, margin);
		fdlDbNameOverride.right = new FormAttachment(middle, -margin);
		wlDbNameOverride.setLayoutData(fdlDbNameOverride);
		wDbNameOverride = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wDbNameOverride);
		wDbNameOverride.addModifyListener(lsMod);
		fdDbNameOverride = new FormData();
		fdDbNameOverride.left = new FormAttachment(middle, 0);
		fdDbNameOverride.top = new FormAttachment(wMaxErrors, margin);
		fdDbNameOverride.right = new FormAttachment(100, 0);
		wDbNameOverride.setLayoutData(fdDbNameOverride);				
		
		// Control file line
		wlControlFile = new Label(shell, SWT.RIGHT);
		wlControlFile.setText(BaseMessages.getString(PKG, "GPLoadDialog.ControlFile.Label")); //$NON-NLS-1$
 		props.setLook(wlControlFile);
		fdlControlFile = new FormData();
		fdlControlFile.left = new FormAttachment(0, 0);
		fdlControlFile.top = new FormAttachment(wDbNameOverride, margin);
		fdlControlFile.right = new FormAttachment(middle, -margin);
		wlControlFile.setLayoutData(fdlControlFile);		
		wbControlFile = new Button(shell, SWT.PUSH | SWT.CENTER);
 		props.setLook(wbControlFile);
		wbControlFile.setText(BaseMessages.getString(PKG, "GPLoadDialog.Browse.Button")); //$NON-NLS-1$
		fdbControlFile = new FormData();
		fdbControlFile.right = new FormAttachment(100, 0);
		fdbControlFile.top = new FormAttachment(wDbNameOverride, margin);
		wbControlFile.setLayoutData(fdbControlFile);
		wControlFile = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);				
 		props.setLook(wControlFile);
		wControlFile.addModifyListener(lsMod);
		fdControlFile = new FormData();
		fdControlFile.left = new FormAttachment(middle, 0);
		fdControlFile.top = new FormAttachment(wDbNameOverride, margin);
		fdControlFile.right = new FormAttachment(wbControlFile, -margin);
		wControlFile.setLayoutData(fdControlFile);		

		// Data file line
		wlDataFile = new Label(shell, SWT.RIGHT);
		wlDataFile.setText(BaseMessages.getString(PKG, "GPLoadDialog.DataFile.Label")); //$NON-NLS-1$
 		props.setLook(wlDataFile);
		fdlDataFile = new FormData();
		fdlDataFile.left = new FormAttachment(0, 0);
		fdlDataFile.top = new FormAttachment(wControlFile, margin);
		fdlDataFile.right = new FormAttachment(middle, -margin);
		wlDataFile.setLayoutData(fdlDataFile);
		wbDataFile = new Button(shell, SWT.PUSH | SWT.CENTER);
 		props.setLook(wbDataFile);
		wbDataFile.setText(BaseMessages.getString(PKG, "GPLoadDialog.Browse.Button")); //$NON-NLS-1$
		fdbDataFile = new FormData();
		fdbDataFile.right = new FormAttachment(100, 0);
		fdbDataFile.top = new FormAttachment(wControlFile, margin);
		wbDataFile.setLayoutData(fdbDataFile);	
		wDataFile = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wDataFile);
		wDataFile.addModifyListener(lsMod);
		fdDataFile = new FormData();
		fdDataFile.left = new FormAttachment(middle, 0);
		fdDataFile.top = new FormAttachment(wControlFile, margin);
		fdDataFile.right = new FormAttachment(wbDataFile, -margin);
		wDataFile.setLayoutData(fdDataFile);
		
		// Log file line
		wlLogFile = new Label(shell, SWT.RIGHT);
		wlLogFile.setText(BaseMessages.getString(PKG, "GPLoadDialog.LogFile.Label")); //$NON-NLS-1$
 		props.setLook(wlLogFile);
		fdlLogFile = new FormData();
		fdlLogFile.left = new FormAttachment(0, 0);
		fdlLogFile.top = new FormAttachment(wDataFile, margin);
		fdlLogFile.right = new FormAttachment(middle, -margin);
		wlLogFile.setLayoutData(fdlLogFile);
		wbLogFile = new Button(shell, SWT.PUSH | SWT.CENTER);
 		props.setLook(wbLogFile);
		wbLogFile.setText(BaseMessages.getString(PKG, "GPLoadDialog.Browse.Button")); //$NON-NLS-1$
		fdbLogFile = new FormData();
		fdbLogFile.right = new FormAttachment(100, 0);
		fdbLogFile.top = new FormAttachment(wDataFile, margin);
		wbLogFile.setLayoutData(fdbLogFile);
		wLogFile = new TextVar(transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
 		props.setLook(wLogFile);
		wLogFile.addModifyListener(lsMod);
		fdLogFile = new FormData();
		fdLogFile.left = new FormAttachment(middle, 0);
		fdLogFile.top = new FormAttachment(wDataFile, margin);
		fdLogFile.right = new FormAttachment(wbLogFile, -margin);
		wLogFile.setLayoutData(fdLogFile);		

		//
        // Control encoding line
        //
        // The drop down is editable as it may happen an encoding may not be present
        // on one machine, but you may want to use it on your execution server
        //
        wlEncoding=new Label(shell, SWT.RIGHT);
        wlEncoding.setText(BaseMessages.getString(PKG, "GPLoadDialog.Encoding.Label"));
        props.setLook(wlEncoding);
        fdlEncoding=new FormData();
        fdlEncoding.left  = new FormAttachment(0, 0);
        fdlEncoding.top   = new FormAttachment(wLogFile, margin);
        fdlEncoding.right = new FormAttachment(middle, -margin);
        wlEncoding.setLayoutData(fdlEncoding);
        wEncoding=new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wEncoding.setToolTipText(BaseMessages.getString(PKG, "GPLoadDialog.Encoding.Tooltip"));
        wEncoding.setItems(encodings);
        props.setLook(wEncoding);
        fdEncoding=new FormData();
        fdEncoding.left = new FormAttachment(middle, 0);
        fdEncoding.top  = new FormAttachment(wlLogFile, margin);
        fdEncoding.right= new FormAttachment(100, 0);        
        wEncoding.setLayoutData(fdEncoding);
        wEncoding.addModifyListener(lsMod);

		// Erase files line
		wlEraseFiles = new Label(shell, SWT.RIGHT);
		wlEraseFiles.setText(BaseMessages.getString(PKG, "GPLoadDialog.EraseFiles.Label")); //$NON-NLS-1$
 		props.setLook(wlEraseFiles);
		fdlEraseFiles = new FormData();
		fdlEraseFiles.left = new FormAttachment(0, 0);
		fdlEraseFiles.top = new FormAttachment(wEncoding, margin);
		fdlEraseFiles.right = new FormAttachment(middle, -margin);
		wlEraseFiles.setLayoutData(fdlEraseFiles);
		wEraseFiles = new Button(shell, SWT.CHECK);
 		props.setLook(wEraseFiles);
		fdEraseFiles = new FormData();
		fdEraseFiles.left = new FormAttachment(middle, 0);
		fdEraseFiles.top = new FormAttachment(wEncoding, margin);
		fdEraseFiles.right = new FormAttachment(100, 0);
		wEraseFiles.setLayoutData(fdEraseFiles);				
		wEraseFiles.addSelectionListener(new SelectionAdapter() 
    	    {
		        public void widgetSelected(SelectionEvent e) 
		        {
				    input.setChanged();
		    	}
	        }
        );
		
		// THE BUTTONS
		wOK = new Button(shell, SWT.PUSH);
		wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); //$NON-NLS-1$
		wSQL = new Button(shell, SWT.PUSH);
		wSQL.setText(BaseMessages.getString(PKG, "GPLoadDialog.SQL.Button")); //$NON-NLS-1$
		wCancel = new Button(shell, SWT.PUSH);
		wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //$NON-NLS-1$

		setButtonPositions(new Button[] { wOK, wSQL, wCancel }, margin, null);

		// The field Table
		wlReturn = new Label(shell, SWT.NONE);
		wlReturn.setText(BaseMessages.getString(PKG, "GPLoadDialog.Fields.Label")); //$NON-NLS-1$
 		props.setLook(wlReturn);
		fdlReturn = new FormData();
		fdlReturn.left = new FormAttachment(0, 0);
		fdlReturn.top = new FormAttachment(wEraseFiles, margin);
		wlReturn.setLayoutData(fdlReturn);

		int UpInsCols = 3;
		int UpInsRows = (input.getFieldTable() != null ? input.getFieldTable().length : 1);

		ciReturn = new ColumnInfo[UpInsCols];
		ciReturn[0] = new ColumnInfo(BaseMessages.getString(PKG, "GPLoadDialog.ColumnInfo.TableField"),ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false); //$NON-NLS-1$
		ciReturn[1] = new ColumnInfo(BaseMessages.getString(PKG, "GPLoadDialog.ColumnInfo.StreamField"), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false); //$NON-NLS-1$
		ciReturn[2] = new ColumnInfo(BaseMessages.getString(PKG, "GPLoadDialog.ColumnInfo.DateMask"), ColumnInfo.COLUMN_TYPE_CCOMBO,
				                     new String[] {"",                //$NON-NLS-1$
			                                       BaseMessages.getString(PKG, "GPLoadDialog.DateMask.Label"),
	                                        	   BaseMessages.getString(PKG, "GPLoadDialog.DateTimeMask.Label")}, true);
		tableFieldColumns.add(ciReturn[0]);
		wReturn = new TableView(transMeta, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
				ciReturn, UpInsRows, lsMod, props);

		wGetLU = new Button(shell, SWT.PUSH);
		wGetLU.setText(BaseMessages.getString(PKG, "GPLoadDialog.GetFields.Label")); //$NON-NLS-1$
		fdGetLU = new FormData();
		fdGetLU.top   = new FormAttachment(wlReturn, margin);
		fdGetLU.right = new FormAttachment(100, 0);
		wGetLU.setLayoutData(fdGetLU);
		
		wDoMapping = new Button(shell, SWT.PUSH);
		wDoMapping.setText(BaseMessages.getString(PKG, "GPLoadDialog.EditMapping.Label")); //$NON-NLS-1$
		fdDoMapping = new FormData();
		fdDoMapping.top   = new FormAttachment(wGetLU, margin);
		fdDoMapping.right = new FormAttachment(100, 0);
		wDoMapping.setLayoutData(fdDoMapping);

		wDoMapping.addListener(SWT.Selection, new Listener() { 	public void handleEvent(Event arg0) { generateMappings();}});

		fdReturn = new FormData();
		fdReturn.left = new FormAttachment(0, 0);
		fdReturn.top = new FormAttachment(wlReturn, margin);
		fdReturn.right = new FormAttachment(wGetLU, -margin);
		fdReturn.bottom = new FormAttachment(wOK, -2*margin);
		wReturn.setLayoutData(fdReturn);
		
	    // 
        // Search the fields in the background
        //
        
        final Runnable runnable = new Runnable()
        {
            public void run()
            {
                StepMeta stepMeta = transMeta.findStep(stepname);
                if (stepMeta!=null)
                {
                    try
                    {
                    	RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);
                        
                        // Remember these fields...
                        for (int i=0;i<row.size();i++)
                        {
                        	inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
                        }
                        
                        setComboBoxes(); 
                    }
                    catch(KettleException e)
                    {
                        logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
                    }
                }
            }
        };
        new Thread(runnable).start();
		

		wbGploadPath.addSelectionListener(new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent e)
            {
                FileDialog dialog = new FileDialog(shell, SWT.OPEN);
                dialog.setFilterExtensions(new String[] { "*" });
                if (wGploadPath.getText() != null)
                {
                    dialog.setFileName(wGploadPath.getText());
                }
                dialog.setFilterNames(ALL_FILETYPES);
                if (dialog.open() != null)
                {
                	wGploadPath.setText(dialog.getFilterPath() + Const.FILE_SEPARATOR
                                      + dialog.getFileName());
                }
            }
        });
			
		wbControlFile.addSelectionListener(new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent e)
            {
                FileDialog dialog = new FileDialog(shell, SWT.OPEN);
                dialog.setFilterExtensions(new String[] { "*" });
                if (wControlFile.getText() != null)
                {
                    dialog.setFileName(wControlFile.getText());
                }
                dialog.setFilterNames(ALL_FILETYPES);               
                if (dialog.open() != null)
                {
                	wControlFile.setText(dialog.getFilterPath() + Const.FILE_SEPARATOR
                                      + dialog.getFileName());
                }
            }
        });		
		
		wbDataFile.addSelectionListener(new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent e)
            {
                FileDialog dialog = new FileDialog(shell, SWT.OPEN);
                dialog.setFilterExtensions(new String[] { "*" });
                if (wDataFile.getText() != null)
                {
                    dialog.setFileName(wDataFile.getText());
                }                
                dialog.setFilterNames(ALL_FILETYPES);
                if (dialog.open() != null)
                {
                	wDataFile.setText(dialog.getFilterPath() + Const.FILE_SEPARATOR
                                      + dialog.getFileName());
                }
            }
        });	

		wbLogFile.addSelectionListener(new SelectionAdapter()
        {
            public void widgetSelected(SelectionEvent e)
            {
                FileDialog dialog = new FileDialog(shell, SWT.OPEN);
                dialog.setFilterExtensions(new String[] { "*" });
                if (wLogFile.getText() != null)
                {
                    dialog.setFileName(wLogFile.getText());
                }                
                dialog.setFilterNames(ALL_FILETYPES);
                if (dialog.open() != null)
                {
                	wLogFile.setText(dialog.getFilterPath() + Const.FILE_SEPARATOR
                                      + dialog.getFileName());
                }
            }
        });
		

		// Add listeners
		lsOK = new Listener()
		{
			public void handleEvent(Event e)
			{
				ok();
			}
		};
		lsGetLU = new Listener()
		{
			public void handleEvent(Event e)
			{
				getUpdate();
			}
		};
		lsSQL = new Listener()
		{
			public void handleEvent(Event e)
			{
				create();
			}
		};
		lsCancel = new Listener()
		{
			public void handleEvent(Event e)
			{
				cancel();
			}
		};

		wOK.addListener(SWT.Selection, lsOK);
		wGetLU.addListener(SWT.Selection, lsGetLU);
		wSQL.addListener(SWT.Selection, lsSQL);
		wCancel.addListener(SWT.Selection, lsCancel);

		lsDef = new SelectionAdapter() { public void widgetDefaultSelected(SelectionEvent e) { ok(); } };

		wStepname.addSelectionListener(lsDef);
        wSchema.addSelectionListener(lsDef);
        wTable.addSelectionListener(lsDef);
        wMaxErrors.addSelectionListener(lsDef);
        wDbNameOverride.addSelectionListener(lsDef);
        wControlFile.addSelectionListener(lsDef);
        wDataFile.addSelectionListener(lsDef);
        wLogFile.addSelectionListener(lsDef);


		// Detect X or ALT-F4 or something that kills this window...
		shell.addShellListener(new ShellAdapter()
		{
			public void shellClosed(ShellEvent e)
			{
				cancel();
			}
		});


		wbTable.addSelectionListener(new SelectionAdapter()
		{
			public void widgetSelected(SelectionEvent e)
			{
				getTableName(wSchema, wTable);
			}
		});
		
	   wbErrorTable.addSelectionListener(new SelectionAdapter()
	      {
	         public void widgetSelected(SelectionEvent e)
	         {
	            getTableName(wSchema, wErrorTable);
	         }
	      });

		// Set the shell size, based upon previous time...
		setSize();

		getData();
		setTableFieldCombo();
		input.setChanged(changed);

		shell.open();
		while (!shell.isDisposed())
		{
			if (!display.readAndDispatch())
				display.sleep();
		}
		return stepname;
	}
	/**
	 * Reads in the fields from the previous steps and from the ONE next step and opens an 
	 * EnterMappingDialog with this information. After the user did the mapping, those information 
	 * is put into the Select/Rename table.
	 */
	private void generateMappings() {

		// Determine the source and target fields...
		//
		RowMetaInterface sourceFields;
		RowMetaInterface targetFields;

		try {
			sourceFields = transMeta.getPrevStepFields(stepMeta);
		} catch(KettleException e) {
			new ErrorDialog(shell, BaseMessages.getString(PKG, "GPLoadDialog.DoMapping.UnableToFindSourceFields.Title"), BaseMessages.getString(PKG, "GPLoadDialog.DoMapping.UnableToFindSourceFields.Message"), e);
			return;
		}
		// refresh data
		input.setDatabaseMeta(transMeta.findDatabase(wConnection.getText()) );
		input.setTableName(transMeta.environmentSubstitute(wTable.getText()));
		StepMetaInterface stepMetaInterface = stepMeta.getStepMetaInterface();
		try {
			targetFields = stepMetaInterface.getRequiredFields(transMeta);
		} catch (KettleException e) {
			new ErrorDialog(shell, BaseMessages.getString(PKG, "GPLoadDialog.DoMapping.UnableToFindTargetFields.Title"), BaseMessages.getString(PKG, "GPLoadDialog.DoMapping.UnableToFindTargetFields.Message"), e);
			return;
		}

		String[] inputNames = new String[sourceFields.size()];
		for (int i = 0; i < sourceFields.size(); i++) {
			ValueMetaInterface value = sourceFields.getValueMeta(i);
			inputNames[i] = value.getName()+
			     EnterMappingDialog.STRING_ORIGIN_SEPARATOR+value.getOrigin()+")";
		}

		// Create the existing mapping list...
		//
		List<SourceToTargetMapping> mappings = new ArrayList<SourceToTargetMapping>();
		StringBuffer missingSourceFields = new StringBuffer();
		StringBuffer missingTargetFields = new StringBuffer();

		int nrFields = wReturn.nrNonEmpty();
		for (int i = 0; i < nrFields ; i++) {
			TableItem item = wReturn.getNonEmpty(i);
			String source = item.getText(2);
			String target = item.getText(1);
			
			int sourceIndex = sourceFields.indexOfValue(source); 
			if (sourceIndex<0) {
				missingSourceFields.append(Const.CR + "   " + source+" --> " + target);
			}
			int targetIndex = targetFields.indexOfValue(target);
			if (targetIndex<0) {
				missingTargetFields.append(Const.CR + "   " + source+" --> " + target);
			}
			if (sourceIndex<0 || targetIndex<0) {
				continue;
			}

			SourceToTargetMapping mapping = new SourceToTargetMapping(sourceIndex, targetIndex);
			mappings.add(mapping);
		}

		// show a confirm dialog if some missing field was found
		//
		if (missingSourceFields.length()>0 || missingTargetFields.length()>0){
			
			String message="";
			if (missingSourceFields.length()>0) {
				message+=BaseMessages.getString(PKG, "GPLoadDialog.DoMapping.SomeSourceFieldsNotFound", missingSourceFields.toString())+Const.CR;
			}
			if (missingTargetFields.length()>0) {
				message+=BaseMessages.getString(PKG, "GPLoadDialog.DoMapping.SomeTargetFieldsNotFound", missingSourceFields.toString())+Const.CR;
			}
			message+=Const.CR;
			message+=BaseMessages.getString(PKG, "GPLoadDialog.DoMapping.SomeFieldsNotFoundContinue")+Const.CR;
			MessageDialog.setDefaultImage(GUIResource.getInstance().getImageSpoon());
			boolean goOn = MessageDialog.openConfirm(shell, BaseMessages.getString(PKG, "GPLoadDialog.DoMapping.SomeFieldsNotFoundTitle"), message);
			if (!goOn) {
				return;
			}
		}
		EnterMappingDialog d = new EnterMappingDialog(GPLoadDialog.this.shell, sourceFields.getFieldNames(), targetFields.getFieldNames(), mappings);
		mappings = d.open();

		// mappings == null if the user pressed cancel
		//
		if (mappings!=null) {
			// Clear and re-populate!
			//
			wReturn.table.removeAll();
			wReturn.table.setItemCount(mappings.size());
			for (int i = 0; i < mappings.size(); i++) {
				SourceToTargetMapping mapping = (SourceToTargetMapping) mappings.get(i);
				TableItem item = wReturn.table.getItem(i);
				item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
				item.setText(1, targetFields.getValueMeta(mapping.getTargetPosition()).getName());
			}
			wReturn.setRowNums();
			wReturn.optWidth(true);
		}
	}
	private void setTableFieldCombo(){
		Runnable fieldLoader = new Runnable() {
			public void run() {
				//clear
				for (int i = 0; i < tableFieldColumns.size(); i++) {
					ColumnInfo colInfo = (ColumnInfo) tableFieldColumns.get(i);
					colInfo.setComboValues(new String[] {});
				}
				if (!Const.isEmpty(wTable.getText())) {
					DatabaseMeta ci = transMeta.findDatabase(wConnection.getText());
					if (ci != null) {
						Database db = new Database(loggingObject, ci);
						try {
							db.connect();

							String schemaTable = ci	.getQuotedSchemaTableCombination(transMeta.environmentSubstitute(wSchema
											.getText()), transMeta.environmentSubstitute(wTable.getText()));
							RowMetaInterface r = db.getTableFields(schemaTable);
							if (null != r) {
								String[] fieldNames = r.getFieldNames();
								if (null != fieldNames) {
									for (int i = 0; i < tableFieldColumns.size(); i++) {
										ColumnInfo colInfo = (ColumnInfo) tableFieldColumns.get(i);
										colInfo.setComboValues(fieldNames);
									}
								}
							}
						} catch (Exception e) {
							for (int i = 0; i < tableFieldColumns.size(); i++) {
								ColumnInfo colInfo = (ColumnInfo) tableFieldColumns	.get(i);
								colInfo.setComboValues(new String[] {});
							}
							// ignore any errors here. drop downs will not be
							// filled, but no problem for the user
						}
					}
				}
			}
		};
		shell.getDisplay().asyncExec(fieldLoader);
	}
	protected void setComboBoxes()
    {
        // Something was changed in the row.
        //
		final Map<String, Integer> fields = new HashMap<String, Integer>();
        
        // Add the currentMeta fields...
        fields.putAll(inputFields);
        
        Set<String> keySet = fields.keySet();
        List<String> entries = new ArrayList<String>(keySet);
        
        String[] fieldNames= (String[]) entries.toArray(new String[entries.size()]);
        Const.sortStrings(fieldNames);
        // return fields
        ciReturn[1].setComboValues(fieldNames);
    }
	/**
	 * Copy information from the meta-data input to the dialog fields.
	 */
	public void getData()
	{
		int i;
		if(log.isDebug()) logDebug(BaseMessages.getString(PKG, "GPLoadDialog.Log.GettingKeyInfo")); //$NON-NLS-1$

		wMaxErrors.setText("" + input.getMaxErrors());   //$NON-NLS-1$

		if (input.getFieldTable() != null)
			for (i = 0; i < input.getFieldTable().length; i++)
			{
				TableItem item = wReturn.table.getItem(i);
				if (input.getFieldTable()[i] != null)
					item.setText(1, input.getFieldTable()[i]);
				if (input.getFieldStream()[i] != null)
					item.setText(2, input.getFieldStream()[i]);
				String dateMask = input.getDateMask()[i];
				if (dateMask!=null) {
					if ( GPLoadMeta.DATE_MASK_DATE.equals(dateMask) )
					{
					    item.setText(3,BaseMessages.getString(PKG, "GPLoadDialog.DateMask.Label"));
					}
					else if ( GPLoadMeta.DATE_MASK_DATETIME.equals(dateMask))
					{
						item.setText(3,BaseMessages.getString(PKG, "GPLoadDialog.DateTimeMask.Label"));
					}
					else 
					{
						item.setText(3,"");
					}					
				} 
				else {
					item.setText(3,"");
				}
			}

		if (input.getDatabaseMeta() != null)
			wConnection.setText(input.getDatabaseMeta().getName());
		else
		{
			if (transMeta.nrDatabases() == 1)
			{
				wConnection.setText(transMeta.getDatabase(0).getName());
			}
		}
        if (input.getSchemaName() != null) wSchema.setText(input.getSchemaName());
		if (input.getTableName() != null) wTable.setText(input.getTableName());
		if (input.getErrorTableName() != null) wErrorTable.setText(input.getErrorTableName());
		if (input.getGploadPath() != null) wGploadPath.setText(input.getGploadPath());
		if (input.getControlFile() != null) wControlFile.setText(input.getControlFile());
		if (input.getDataFile() != null) wDataFile.setText(input.getDataFile());
		if (input.getLogFile() != null) wLogFile.setText(input.getLogFile());
		if (input.getEncoding() != null) wEncoding.setText(input.getEncoding());
		if (input.getDbNameOverride() != null ) wDbNameOverride.setText(input.getDbNameOverride());
		wEraseFiles.setSelection(input.isEraseFiles());
		
		String method = input.getLoadMethod();
		//if ( GPLoadMeta.METHOD_AUTO_CONCURRENT.equals(method) )
		//{
		//	wLoadMethod.select(0);
		//}
		if ( GPLoadMeta.METHOD_AUTO_END.equals(method) )
		{
			wLoadMethod.select(0);
		}
		else if ( GPLoadMeta.METHOD_MANUAL.equals(method) )
		{
			wLoadMethod.select(1);
		}
		else  
		{
			if(log.isDebug()) logDebug("Internal error: load_method set to default 'auto at end'"); //$NON-NLS-1$
			wLoadMethod.select(0);
		}		
		
		String action = input.getLoadAction();
		if ( GPLoadMeta.ACTION_INSERT.equals(action))
		{
			wLoadAction.select(0);
		}
		else if ( GPLoadMeta.ACTION_UPDATE.equals(action))
		{
			wLoadAction.select(1);
		}
		else if ( GPLoadMeta.ACTION_MERGE.equals(action))
		{
			wLoadAction.select(2);
		}
		else
		{
			if(log.isDebug()) logDebug("Internal error: load_action set to default '"+GPLoadMeta.ACTION_INSERT+"'"); //$NON-NLS-1$
    		wLoadAction.select(0);
		}
		
		
		wStepname.selectAll();
		wReturn.setRowNums();
		wReturn.optWidth(true);
	}
	
	private void cancel()
	{
		stepname = null;
		input.setChanged(changed);
		dispose();
	}

	private void getInfo(GPLoadMeta inf)
	{
		int nrfields = wReturn.nrNonEmpty();

		inf.allocate(nrfields);

		inf.setMaxErrors( Const.toInt(wMaxErrors.getText(), 0) );

		inf.setDbNameOverride(wDbNameOverride.getText());

		if(log.isDebug()) logDebug(BaseMessages.getString(PKG, "GPLoadDialog.Log.FoundFields", "" + nrfields)); //$NON-NLS-1$ //$NON-NLS-2$
		for (int i = 0; i < nrfields; i++)
		{
			TableItem item = wReturn.getNonEmpty(i);
			inf.getFieldTable()[i] = item.getText(1);
			inf.getFieldStream()[i] = item.getText(2);
			if ( BaseMessages.getString(PKG, "GPLoadDialog.DateMask.Label").equals(item.getText(3)) )
 			    inf.getDateMask()[i] = GPLoadMeta.DATE_MASK_DATE;
			else if ( BaseMessages.getString(PKG, "GPLoadDialog.DateTimeMask.Label").equals(item.getText(3)) )
				inf.getDateMask()[i] = GPLoadMeta.DATE_MASK_DATETIME;
			else inf.getDateMask()[i] = "";
		}

        inf.setSchemaName( wSchema.getText() );
		inf.setTableName( wTable.getText() );
		inf.setErrorTableName(wErrorTable.getText());
		inf.setDatabaseMeta(  transMeta.findDatabase(wConnection.getText()) );
		inf.setGploadPath( wGploadPath.getText() );
		inf.setControlFile( wControlFile.getText() );
		inf.setDataFile( wDataFile.getText() );
		inf.setLogFile( wLogFile.getText() );
		inf.setEncoding( wEncoding.getText() );
		inf.setEraseFiles( wEraseFiles.getSelection() );

		/*
		 * Set the loadmethod
		 */
		String method = wLoadMethod.getText();
		//if ( BaseMessages.getString(PKG, "GPLoadDialog.AutoConcLoadMethod.Label").equals(method) )
		//{
		//	inf.setLoadMethod(GPLoadMeta.METHOD_AUTO_CONCURRENT);
		//}
		if ( BaseMessages.getString(PKG, "GPLoadDialog.AutoEndLoadMethod.Label").equals(method) )
		{
			inf.setLoadMethod(GPLoadMeta.METHOD_AUTO_END);
		}
		else if ( BaseMessages.getString(PKG, "GPLoadDialog.ManualLoadMethod.Label").equals(method) )
		{
			inf.setLoadMethod(GPLoadMeta.METHOD_MANUAL);
		}
		else  
		{
			if(log.isDebug()) logDebug("Internal error: load_method set to default 'auto concurrent', value found '" + method + "'."); //$NON-NLS-1$
			inf.setLoadMethod(GPLoadMeta.METHOD_AUTO_END);
		}	
		
		/*
		 * Set the load action 
		 */
		String action = wLoadAction.getText();
		if ( BaseMessages.getString(PKG, "GPLoadDialog.InsertLoadAction.Label").equals(action) )
		{
			inf.setLoadAction(GPLoadMeta.ACTION_INSERT);
		}
		else if ( BaseMessages.getString(PKG, "GPLoadDialog.UpdateLoadAction.Label").equals(action) )
		{
			inf.setLoadAction(GPLoadMeta.ACTION_UPDATE);
		}
		else if ( BaseMessages.getString(PKG, "GPLoadDialog.MergeLoadAction.Label").equals(action) )
		{
			inf.setLoadAction(GPLoadMeta.ACTION_MERGE);
		}
		else
		{
			if(log.isDebug()) logDebug("Internal error: load_action set to default 'append', value found '" + action + "'."); //$NON-NLS-1$
			inf.setLoadAction(GPLoadMeta.ACTION_INSERT);
		}

		stepname = wStepname.getText(); // return value
	}

	private void ok()
	{
		if (Const.isEmpty(wStepname.getText())) return;

		// Get the information for the dialog into the input structure.
		getInfo(input);

		if (input.getDatabaseMeta() == null)
		{
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
			mb.setMessage(BaseMessages.getString(PKG, "GPLoadDialog.InvalidConnection.DialogMessage")); //$NON-NLS-1$
			mb.setText(BaseMessages.getString(PKG, "GPLoadDialog.InvalidConnection.DialogTitle")); //$NON-NLS-1$
			mb.open();
		}

		dispose();
	}

	private void getTableName(TextVar schema, TextVar tableName)
	{
		DatabaseMeta inf = null;
		// New class: SelectTableDialog
		int connr = wConnection.getSelectionIndex();
		if (connr >= 0)
			inf = transMeta.getDatabase(connr);

		if (inf != null)
		{
			if(log.isDebug()) logDebug(BaseMessages.getString(PKG, "GPLoadDialog.Log.LookingAtConnection") + inf.toString()); //$NON-NLS-1$

			DatabaseExplorerDialog std = new DatabaseExplorerDialog(shell, SWT.NONE, inf, transMeta.getDatabases());
      std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
			if (std.open())
			{
                schema.setText(Const.NVL(std.getSchemaName(), ""));
                tableName.setText(Const.NVL(std.getTableName(), ""));
			}
		}
		else
		{
			MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
			mb.setMessage(BaseMessages.getString(PKG, "GPLoadDialog.InvalidConnection.DialogMessage")); //$NON-NLS-1$
			mb.setText(BaseMessages.getString(PKG, "GPLoadDialog.InvalidConnection.DialogTitle")); //$NON-NLS-1$
			mb.open();
		}
	}
	
	private void getUpdate()
	{
		try
		{
			RowMetaInterface r = transMeta.getPrevStepFields(stepname);
			if (r != null)
			{
                TableItemInsertListener listener = new TableItemInsertListener()
                {
                    public boolean tableItemInserted(TableItem tableItem, ValueMetaInterface v)
                    {
                    	if ( v.getType() == ValueMetaInterface.TYPE_DATE )
                    	{
                    		// The default is date mask.
                    		tableItem.setText(3, BaseMessages.getString(PKG, "GPLoadDialog.DateMask.Label"));
                    	}
                    	else
                    	{
                            tableItem.setText(3, "");
                    	}
                        return true;
                    }
                };
                BaseStepDialog.getFieldsFromPrevious(r, wReturn, 1, new int[] { 1, 2}, new int[] {}, -1, -1, listener);
			}
		}
		catch (KettleException ke)
		{
			new ErrorDialog(shell, BaseMessages.getString(PKG, "GPLoadDialog.FailedToGetFields.DialogTitle"), //$NON-NLS-1$
					BaseMessages.getString(PKG, "GPLoadDialog.FailedToGetFields.DialogMessage"), ke); //$NON-NLS-1$
		}
	}

	// Generate code for create table...
	// Conversions done by Database
	private void create()
	{
		try
		{
			GPLoadMeta info = new GPLoadMeta();
			getInfo(info);

			String name = stepname; // new name might not yet be linked to other steps!
			StepMeta stepMeta = new StepMeta(BaseMessages.getString(PKG, "GPLoadDialog.StepMeta.Title"), name, info); //$NON-NLS-1$
			RowMetaInterface prev = transMeta.getPrevStepFields(stepname);

			SQLStatement sql = info.getSQLStatements(transMeta, stepMeta, prev);
			if (!sql.hasError())
			{
				if (sql.hasSQL())
				{
					SQLEditor sqledit = new SQLEditor(shell, SWT.NONE, info.getDatabaseMeta(), transMeta.getDbCache(),
							sql.getSQL());
					sqledit.open();
				}
				else
				{
					MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
					mb.setMessage(BaseMessages.getString(PKG, "GPLoadDialog.NoSQLNeeds.DialogMessage")); //$NON-NLS-1$
					mb.setText(BaseMessages.getString(PKG, "GPLoadDialog.NoSQLNeeds.DialogTitle")); //$NON-NLS-1$
					mb.open();
				}
			}
			else
			{
				MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
				mb.setMessage(sql.getError());
				mb.setText(BaseMessages.getString(PKG, "GPLoadDialog.SQLError.DialogTitle")); //$NON-NLS-1$
				mb.open();
			}
		}
		catch (KettleException ke)
		{
			new ErrorDialog(shell, BaseMessages.getString(PKG, "GPLoadDialog.CouldNotBuildSQL.DialogTitle"), //$NON-NLS-1$
					BaseMessages.getString(PKG, "GPLoadDialog.CouldNotBuildSQL.DialogMessage"), ke); //$NON-NLS-1$
		}

	}
}