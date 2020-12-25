import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_cytoscape as cyto
from dash.dependencies import Input, Output, State
import webbrowser
from urllib.parse import unquote
from subprocess import Popen, PIPE
import os
import json
import time


##################################################
####         Main Variable definition         ####
##################################################

working_directory = os.path.dirname(__file__)

app = dash.Dash(__name__)
app.title = "DAG & Co."

Version = "_v3_1"


##################################################
####               User Interface             ####
##################################################

app.layout = html.Div([
    
    # ===================================
    # ====         Left panel        ====
    # ===================================

    html.Div(
        
        id="Left_panel",

        children=[
            
            # -----------------------------------
            # ----          Title            ----
            # -----------------------------------

            html.H1(
                "DAG & CO",
                id="Left_panel_title",
                className="Left_panel_title_format"
            ),

            html.H5(
                f"Wordcount MapReduce (v{Version[-3]}.{Version[-1]})",
                id="Left_panel_subtitle",
                className="Left_panel_subtitle_format"
            ),

            html.H5(
                "by Mathias NOURRY",
                id="Signature",
                className="Signature_format"
            ),

            # -----------------------------------
            # ----       Input File         ----
            # -----------------------------------

            # Instruction for the list of remote computers
            html.H5(
                "Enter the list of remote computers with absolute path :",
                id="Remote_Computers_Instruction",
                className="Remote_Computers_Instruction_format"
            ),

            # Input for the list of remote computers
            dcc.Input(
                id="Remote_Computers_Input",
                placeholder="Enter filename...",
                className="Remote_Computers_Input_format"
            ),

            # Instruction for the username
            html.H5(
                "Enter the username to login to remote computers :",
                id="Username_Instruction",
                className="Username_Instruction_format"
            ),

            # Input for the username
            dcc.Input(
                id="Username_Input",
                placeholder="Enter Username...",
                className="Username_Input_format"
            ),

            # Instruction for the input filename
            html.H5(
                "Enter the input filename with absolute path :",
                id="Inputfilename_Instruction",
                className="Inputfilename_Instruction_format"
            ),

            # Input for the input filename
            dcc.Input(
                id="Inputfilename_Input",
                placeholder="Enter filename...",
                className="Inputfilename_Input_format"
            ),

            # Run button to launch Wordcount job
            html.Button(
                "Run",
                id="Run_Button",
                className="Run_Button_format"
            ),

            # Store the answer to launh or not the Worcount job
            dcc.Store(
                id="Launch_instruction_Store",
                storage_type='session'
            ),

            # Purge button to purge the output of the previous
            # Wordcount job
            html.Button(
                "Purging the app.",
                id="Purge_Button",
                className="Purge_Button_format"
            ),

            # Div to inform if the intputfile has been accepted
            html.Div(
                id="Inputfilename_Info",
                className="Inputfilename_Info_default_format"
            ),

            # Loading to inform about the course of the 
            # Wordcount job
            dcc.Loading(
                id="Run_Loading",
                color="#b40a34",
                style={
                    'top': '60vh',
                    'left': '6vw',
                    'position': 'fixed'
                }
            ),

            # Interval to refresh the read of ouputs
            # from the Wordcount job
            dcc.Interval(
                id="Countdown_MAPREDUCE_Interval",
                interval=2*1000,
                n_intervals=0
            ),

            # -----------------------------------
            # ----       Logo Telecom        ----
            # -----------------------------------

            html.Div(

                id="LogoTelecom_Img",

                children=[
                    html.Img(
                        src=app.get_asset_url('Logo_Telecom.png'),
                        width='50%',
                        height='37%'
                    )
                ],

                className="LogoTelecom_Img_format"
                
            )

        ],
        
        className="Left_panel_format"

    ),

    # ===================================
    # ====         Main panel        ====
    # ===================================

    html.Div(

        id="Main_panel",

        children=[

            # -----------------------------------
            # ----       Introduction        ----
            # -----------------------------------

            html.Div(
                id="Introduction_Div",
                children=[

                    html.H2("INF727"),
                    html.H2("Systèmes répartis pour le Big Data"),
                    html.Div([
                        "The aim of this application is to see dynamically the evolution of a process. ",
                        "Here, the subject is to count occurrences of every words in a given file. "
                        "This task is made by a MAP-REDUCE system coded in python."
                    ])

                ],
                className="Introduction_Div_format"
            ),

            # -----------------------------------
            # ----           DAG             ----
            # -----------------------------------

            html.Div(
                id="DAG_Div",
                hidden=True,
                children=[
                                        
                    html.Div(
                        id="DAG_figure_Div",
                        children=[

                            # DAG structure
                            cyto.Cytoscape(
                                id='DAG_structure',
                                layout={'name': 'preset'},
                                minZoom=1/2,
                                maxZoom=2,
                                style={'width': '100%', 'height': '100%'},
                                stylesheet=[
                                    {
                                        'selector': '.OK_DAG_node',
                                        'style': {
                                            'label': 'data(label)', 
                                            'width': '20%', 
                                            'height': '20%',
                                            'line-color': '#65e665',
                                            'background-color': '#65e665'
                                            }
                                    },
                                    {
                                        'selector': '.Warning_DAG_node',
                                        'style': {
                                            'label': 'data(label)', 
                                            'width': '20%', 
                                            'height': '20%',
                                            'line-color': '#fcc072',
                                            'background-color': '#fcc072'
                                            }
                                    },
                                    {
                                        'selector': '.Error_DAG_node',
                                        'style': {
                                            'label': 'data(label)', 
                                            'width': '20%', 
                                            'height': '20%',
                                            'line-color': '#fc7272',
                                            'background-color': '#fc7272'
                                            }
                                    },
                                    {
                                        'selector': '.Neutral_DAG_node',
                                        'style': {
                                            'label': 'data(label)', 
                                            'width': '20%', 
                                            'height': '20%',
                                            'line-color': '#5297ff',
                                            'background-color': '#5297ff'
                                            }
                                    },
                                    {
                                        'selector': '.DAG_line',
                                        'style': {
                                            'curve-style': 'bezier', 
                                            'target-arrow-shape': 'vee'
                                            }
                                    },
                                    {
                                        'selector': '.Time_node',
                                        'style': {
                                            'content': 'data(label)', 
                                            'shape':'diamond'
                                            }
                                    },
                                    {
                                        'selector': '.Time_line',
                                        'style': {
                                            'label': 'data(weight)', 
                                            'width': 20, 
                                            'curve-style': 'bezier', 
                                            'target-arrow-shape': 'vee', 
                                            'target-arrow-color': '#dedede', 
                                            'line-color': '#dedede'
                                            }
                                    }

                                ]
                            )

                        ],
                        className="DAG_figure_Div_format"
                    )

                ],
                className='DAG_Div_format'
                ),

            # -----------------------------------
            # ----           LOG             ----
            # -----------------------------------

            html.Div(
                id="LOG_Div",
                hidden=True,
                children=[
                    
                    dcc.Textarea(
                        id="Terminal_Text",
                        disabled=True,
                        draggable=False,
                        className="Terminal_Text_format"
                    )

                ],
                className='LOG_Div_format'
                ),
                
        ],
        
        className="Main_panel_format"

    )

    ]
)


##################################################
####                 Callbacks                ####
##################################################

# ===================================
# ====      Enable view on       ====
# ====        DAG and Log        ====
# ===================================

@app.callback(
    [
        Output('Inputfilename_Info', 'children'),
        Output('Inputfilename_Info', 'className'),
        Output('Introduction_Div', 'hidden'),
        Output('DAG_Div', 'hidden'),
        Output('LOG_Div', 'hidden'),
        Output('Launch_instruction_Store', 'data')
    ],
    [
        Input('Run_Button', 'n_clicks'),
        Input('Purge_Button', 'n_clicks')
    ],
    [
        State('Remote_Computers_Input', 'value'),
        State('Username_Input', 'value'),
        State('Inputfilename_Input', 'value')
    ]
)
def Enable_DAG_Log(Run_click, Purge_click, Remote_computers_filename, Username, Input_filename):

    # If the Purge button is pressed
    if Purge_click is not None:

        return [
            None, 
            "Inputfilename_Info_default_format",
            False, True, True,
            "Stop"
            ]

    else:

        # If the Run button isn't pressed
        if Run_click is None:

            return [
                None, 
                "Inputfilename_Info_default_format",
                False, True, True,
                "Stop"
                ]
        
        # Else if the Run button is pressed
        else:

            # If the Run button is pressed 
            # but the remote computers file isn't entered
            if Remote_computers_filename is None or Remote_computers_filename == "":

                return [
                    "Exception: No remote computers file.", 
                    "Inputfilename_Info_Error_format",
                    False, True, True,
                    "Stop"
                    ]

            # Else if the Run button is pressed 
            # and the remote computers file is entered
            else:

                Remote_computers_filename = unquote(Remote_computers_filename, encoding='utf-8')

                # If the Run button is pressed 
                # and the remote computers file is entered but the remote computers file doesn't exist
                if not os.path.exists(Remote_computers_filename):

                    return [
                        "Exception: The remote computers file doesn't exist.", 
                        "Inputfilename_Info_Error_format",
                        False, True, True,
                        "Stop"
                        ]

                # Else if the Run button is pressed 
                # and the remote computers file is entered and the remote computers file exist
                else:

                    # If the Run button is pressed 
                    # and the remote computers file is entered and the remote computers file exist
                    # but the username isn't entered
                    if Username is None or Username == "":

                        return [
                            "Exception: No username.", 
                            "Inputfilename_Info_Error_format",
                            False, True, True,
                            "Stop"
                            ]

                    # Else if the Run button is pressed 
                    # and the remote computers file is entered and the remote computers file exist
                    # and the username is entered
                    else:

                        # If the Run button is pressed 
                        # and the remote computers file is entered and the remote computers file exist
                        # and the username is entered
                        # but the input file isn't entered
                        if Input_filename is None or Input_filename == "":

                            return [
                                "Exception: No input file.", 
                                "Inputfilename_Info_Error_format",
                                False, True, True,
                                "Stop"
                                ]

                        # Else if the Run button is pressed 
                        # and the remote computers file is entered and the remote computers file exist
                        # and the username is entered
                        # and the input file is entered
                        else:

                            Input_filename = unquote(Input_filename, encoding='utf-8')

                            accepted_format = ['.txt', '.wet']
                            format_respected = [
                                'yes' 
                                if format in Input_filename else 'no'
                                for format in accepted_format
                                ]

                            # If the Run button is pressed 
                            # and the remote computers file is entered and the remote computers file exist
                            # and the username is entered
                            # and the input file is entered but the input file doesn't respect the format
                            if 'yes' not in format_respected:

                                return [
                                    "Exception: Format unrecognized.\nFormat recognised: .txt, .wet", 
                                    "Inputfilename_Info_Error_format",
                                    False, True, True,
                                    "Stop"
                                    ]

                            # Else if the Run button is pressed 
                            # and the remote computers file is entered and the remote computers file exist
                            # and the username is entered
                            # and the input file is entered and the input file respect the format but the input file doesn't exist
                            elif 'yes' in format_respected and not os.path.exists(Input_filename):

                                return [
                                    "Exception: The input file doesn't exist.", 
                                    "Inputfilename_Info_Error_format",
                                    False, True, True,
                                    "Stop"
                                    ]
                            
                            # Else if the Run button is pressed 
                            # and the remote computers file is entered and the remote computers file exist
                            # and the username is entered
                            # and the input file is entered and the input file respect the format and the input file exist
                            else:

                                return [
                                    f"File correctly upload.", 
                                    "Inputfilename_Info_OK_format",
                                    True, False, False,
                                    "Run"
                                    ]



# ===================================
# ====       Run Wordcount       ====
# ===================================

@app.callback(
    [
        Output('Run_Loading', 'children')
    ],
    [
        Input('Launch_instruction_Store', 'modified_timestamp')
    ],
    [
        State('Launch_instruction_Store', 'data'),
        State('Remote_Computers_Input', 'value'),
        State('Username_Input', 'value'),
        State('Inputfilename_Input', 'value')
    ]
)
def Run_Wordcount_process(
    Launch_instruction_Store_modified, Launch_instruction_Store_data,
    Remote_Computers_filename, Username, Input_filename
    ):

    if Launch_instruction_Store_modified is not None and Launch_instruction_Store_data == "Run":

        # Encoding path in utf-8
        Remote_Computers_filename = unquote(Remote_Computers_filename, encoding='utf-8')
        Input_filename = unquote(Input_filename, encoding='utf-8')

        if not os.path.exists(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output'):
            os.mkdir(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output')
            os.mkdir(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/Reduces')

        else:
            # Purge files from Reduce
            if os.path.exists(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/Reduces'):
                list_Reduce_files = os.listdir(os.getcwd() + f"/MAP_REDUCE_process{Version}/Output/Reduces")
                if list_Reduce_files != 0:
                    for file in list_Reduce_files:
                        os.remove(os.getcwd() + f"/MAP_REDUCE_process{Version}/Output/Reduces/{file}")

            # Purge log file LOG.txt
            if os.path.exists(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/LOG.txt'):
                os.remove(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/LOG.txt')

            # Purge DAG file DAG.txt
            if os.path.exists(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/DAG.txt'):
                os.remove(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/DAG.txt')

        Wordcount_process = Popen(
            f"cd {working_directory}/MAP_REDUCE_process{Version} && ./MASTER{Version}.py -i {Input_filename} -rc {Remote_Computers_filename} -u {Username}",
            shell=True
        )
        Wordcount_process.wait()
        Wordcount_process.kill()

        time.sleep(2)

        return [None]

    elif Launch_instruction_Store_modified is not None and Launch_instruction_Store_data == "Stop":

        # If the Wordcount is still running and the user pressed Purge button
        # we need to kill the process
        try:
            
            See_running_process = Popen(['ps', '-a'], stdout=PIPE)
            output = See_running_process.communicate()[0]

            for line in output.splitlines():
                if "python3" in str(line) or "sh" in str(line):
                    os.kill(int(line.split()[0]), 9)

        except:
            
            pass
            
        return [None]

    else:

        return [None]
                       

# ===================================
# ====        Log and DAG        ====
# ====        refreshment        ====
# ===================================

@app.callback(
    [
        Output('DAG_structure', 'elements'),
        Output('Terminal_Text', 'value'),
        Output('Countdown_MAPREDUCE_Interval', 'max_intervals')
    ],
    [
        Input('Countdown_MAPREDUCE_Interval', 'n_intervals'),
        Input('Run_Button', 'n_clicks')
    ],
    [
        State('Inputfilename_Info', 'children')
    ]
)
def Refresh_LOG(Countdown_MAPREDUCE, Run_click, Inputfilename_Info):

    if Run_click is not None:
        
        if Inputfilename_Info == "File correctly upload." and os.path.exists(working_directory + f"/MAP_REDUCE_process{Version}/Output/LOG.txt"):

            # Refresh DAG 
            DAG_elements = []
            for line in open(working_directory + f"/MAP_REDUCE_process{Version}/Output/DAG.txt").readlines():
                DAG_elements.append(
                    json.loads(line)
                )

            # Refresh Log to show in the artificial terminal
            LOG_information = open(working_directory + f"/MAP_REDUCE_process{Version}/Output/LOG.txt").read()

            if "WORDCOUNT TERMINATE" not in LOG_information:

                return [DAG_elements, LOG_information, -1]

            else:
                
                return [DAG_elements, LOG_information, 0]

        else:

            return [[], None, -1]

    else:

        return [[], "", 0]


# ===================================
# ====   Purge the application   ====
# ===================================

@app.callback(
    [
        Output('Run_Button', 'n_clicks'),
        Output('Inputfilename_Input', 'value')
    ],
    [
        Input('Purge_Button', 'n_clicks')
    ]
)
def Purge_app(Purge_click):

    # Purge files from Reduce
    if os.path.exists(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/Reduces'):
        list_Reduce_files = os.listdir(os.getcwd() + f"/MAP_REDUCE_process{Version}/Output/Reduces")
        if list_Reduce_files != 0:
            for file in list_Reduce_files:
                os.remove(os.getcwd() + f"/MAP_REDUCE_process{Version}/Output/Reduces/{file}")

    # Purge log file LOG.txt
    if os.path.exists(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/LOG.txt'):
        os.remove(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/LOG.txt')

    # Purge DAG file DAG.txt
    if os.path.exists(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/DAG.txt'):
        os.remove(os.getcwd() + f'/MAP_REDUCE_process{Version}/Output/DAG.txt')

    return [None,""] 


##################################################
####           Application launching          ####
##################################################

if __name__ == '__main__':
    webbrowser.open("http://127.0.0.1:8080/")
    app.run_server(debug=True, port="8080")