const path = require('path');

   module.exports = {
     entry: {
         // add entries per page we want an entry point for (like /home, /search, ..)
         home: './app/svelte/home.js',
       },
       output: {
         path: path.resolve(__dirname, 'public/javascripts'),
         filename: '[name].bundle.js'
       },
     resolve: {
       alias: {
         svelte: path.resolve('node_modules', 'svelte/src/runtime')
       },
       extensions: ['.mjs', '.js', '.svelte'],
       mainFields: ['svelte', 'browser', 'module', 'main'],
       conditionNames: ['svelte']
     },
     module: {
       rules: [
         {
           test: /\.svelte$/,
           use: {
             loader: 'svelte-loader',
             options: {
               emitCss: true,
               hotReload: true
             }
           }
         }
       ]
     }
   };

