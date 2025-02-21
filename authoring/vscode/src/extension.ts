// Approach:
//
// 1. parse assignment statements in the python file with regex
// 2. get hover information for the variable left of assignment
// 3. check if it is one of the target types
// 4. if yes, add a code lens to the assignment statement
// 5. on click of the code lens, run send the eval command to the terminal

import * as vscode from 'vscode';
import * as path from 'path';

export class EvalCodeLensProvider implements vscode.CodeLensProvider {
    private variableTypes = new Map<string, string>();
    private _onDidChangeCodeLenses: vscode.EventEmitter<void> = new vscode.EventEmitter<void>();
    public readonly onDidChangeCodeLenses: vscode.Event<void> = this._onDidChangeCodeLenses.event;

    public updateVariableType(variable: string, type: string) {
        this.variableTypes.set(variable, type);
        this._onDidChangeCodeLenses.fire();
    }

    public async provideCodeLenses(document: vscode.TextDocument): Promise<vscode.CodeLens[]> {
        const codeLenses: vscode.CodeLens[] = [];
        const text = document.getText();

        // Find all variable assignments
        const varRegex = /^([a-zA-Z_][a-zA-Z0-9_]*)\s*=/gm;
        let match;

        while ((match = varRegex.exec(text))) {
            const line = document.positionAt(match.index).line;
            const position = new vscode.Position(line, 0);

            // Get hover information for the variable
            const hover = await vscode.commands.executeCommand<vscode.Hover[]>(
                'vscode.executeHoverProvider',
                document.uri,
                position
            );

            if (hover && hover[0]) {
                const hoverContent = hover[0].contents.map(content => 
                    typeof content === 'string' ? content : content.value
                ).join('\n');

                // Check if it's one of our target types
                const isTargetType = /(GroupBy|Join|Source|StagingQuery)(?![\w])/.test(hoverContent);
                if (isTargetType) {
                    const range = new vscode.Range(
                        line, 0,
                        line, match[0].length
                    );

                    codeLenses.push(new vscode.CodeLens(range, {
                        title: "▶ Eval",
                        command: 'python-eval-button.runEval',
                        arguments: [document, line]
                    }));
                }
            }
        }

        return codeLenses;
    }
}

export function activate(context: vscode.ExtensionContext) {
    const codeLensProvider = new EvalCodeLensProvider();

    // Register the CodeLens provider
    context.subscriptions.push(
        vscode.languages.registerCodeLensProvider(
            { language: 'python', scheme: 'file' },
            codeLensProvider
        )
    );

    // Register the command
    context.subscriptions.push(
        vscode.commands.registerCommand('python-eval-button.runEval', async (document: vscode.TextDocument, line: number) => {
            const editor = vscode.window.activeTextEditor;
            if (!editor) return;

            const lineText = editor.document.lineAt(line).text;
            const match = lineText.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*=/);
            
            if (match) {
                const varName = match[1];
                const hover = await vscode.commands.executeCommand<vscode.Hover[]>(
                    'vscode.executeHoverProvider',
                    editor.document.uri,
                    new vscode.Position(line, 0)
                );

                if (hover && hover[0]) {
                    const hoverContent = hover[0].contents.map(content => 
                        typeof content === 'string' ? content : content.value
                    ).join('\n');

                    if (/(GroupBy|Join|Source|StagingQuery)(?![\w])/.test(hoverContent)) {
                        const filePath = editor.document.uri.fsPath;
                        console.log('Running eval for var:', varName);
                        console.log('Full file path:', filePath);

                        // Get the workspace folder (Python package root)
                        const workspaceFolder = vscode.workspace.getWorkspaceFolder(editor.document.uri);
                        if (!workspaceFolder) {
                            console.error('File must be in a workspace');
                            return;
                        }

                        const workspacePath = workspaceFolder.uri.fsPath;
                        console.log('Workspace root:', workspacePath);

                        // Get path relative to workspace root
                        const relPath = path.relative(workspacePath, filePath);
                        console.log('Path relative to root:', relPath);

                        // Convert to Python module path
                        const pythonPath = relPath.replace('.py', '').split(path.sep).join('.');
                        console.log('Final Python import path:', pythonPath);

                        const terminal = vscode.window.activeTerminal || vscode.window.createTerminal('Python Eval');
                        terminal.show();
                        terminal.sendText(`python3 -c "from ${pythonPath} import ${varName}; from ai.chronon.eval import eval; eval(${varName})"`);
                    }
                }
            }
        })
    );
}

export function deactivate() {}